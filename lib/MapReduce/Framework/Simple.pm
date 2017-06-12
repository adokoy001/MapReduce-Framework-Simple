package MapReduce::Framework::Simple;
use 5.010001;
use strict;
use warnings;
use B::Deparse;
use Mouse;
use Data::MessagePack;
use Parallel::ForkManager;
use Plack::Request;
use WWW::Mechanize;
use List::Util qw(shuffle);

our $VERSION = "0.07";

has 'verify_hostname' => (is => 'rw', isa => 'Int', default => 1);
has 'skip_undef_result' => (is => 'rw', isa => 'Int', default => 1);
has 'warn_discarded_data' => (is => 'rw', isa => 'Int', default => 1);
has 'die_discarded_data' => (is => 'rw', isa => 'Int', default => 0);
has 'worker_log' => (is => 'rw', isa => 'Int', default => 0);
has 'force_plackup' => (is => 'rw', isa => 'Int', default => 0);

# To make load balanced data.
sub create_assigned_data {
    my $self = shift;
    my $data = shift;
    my $servers = shift;
    my $options = shift;
    my $chunk_num = 10;
    my $method = 'volume_uniform';
    if(defined($options)){
	if(defined($options->{chunk_num})){
	    $chunk_num = $options->{chunk_num};
	}
	if(defined($options->{method})){
	    $method = $options->{method};
	}
    }
    my $output;
    if($method eq 'element_shuffle'){

	@$data = shuffle(@$data);
	for(0 .. $#$data){
	    push(@{$output->[$_ % $chunk_num]->[0]},$data->[$_]);
	}
	for(0 .. $#$output){
	    $output->[$_]->[1] = $servers->[$_ % scalar(@$servers)];
	}
    }elsif($method eq 'element_sequential'){
	for(0 .. $#$data){
	    push(@{$output->[$_ % $chunk_num]->[0]},$data->[$_]);
	}
	for(0 .. $#$output){
	    $output->[$_]->[1] = $servers->[$_ % scalar(@$servers)];
	}
    }else{
	my $mp = Data::MessagePack->new();
	@$data = map $_->[0],
	    sort {$a->[1] <=> $b->[1]} map [$_, bytes::length $mp->pack($_)],
	    @$data;
	for(0 .. $#$data){
	    push(@{$output->[$_ % $chunk_num]->[0]},$data->[$_]);
	}
	for(0 .. $#$output){
	    $output->[$_]->[1] = $servers->[$_ % scalar(@$servers)];
	}
    }
    return($output);
}

# MapReduce client(Master)
sub map_reduce {
    my $self = shift;
    my $data = shift;
    my $mapper_ref = shift;
    my $reducer_ref = shift;
    my $max_proc = shift;
    my $options = shift;
    my $remote_flg = 1;
    if(defined($options) and defined($options->{remote})){
	$remote_flg = $options->{remote};
    }
    my $stringified_code = B::Deparse->new->coderef2text($mapper_ref);
    my $result;
    my $succeeded_remotes;
    my $failed_remotes;
    my $failed_data;
    my $discarded_data;
    my $pm = Parallel::ForkManager->new($max_proc);
    $pm->run_on_finish(
	sub {
	    my ($pid, $exit_code, $ident, $exit_signal, $core_dump, $data_structure) = @_;
	    if (defined $data_structure) {
		if($data_structure->{is_success} == 1){
		    $succeeded_remotes->{$data_structure->{remote}} = 1;
		    $result->[$data_structure->{id}] = $data_structure->{result};
		}else{
		    $failed_remotes->{$data_structure->{remote}} = 1;
		    push(@$failed_data,$data_structure->{failed_data});
		    $result->[$data_structure->{id}] = undef;
		}
	    }
	}
       );
    if($remote_flg == 1){
	for(my $k=0; $k <= $#$data; $k++){
	    $pm->start and next;
	    my $payload = _perl_to_msgpack(
		{
		    data => $data->[$k]->[0],
		    code => $stringified_code
		   }
	       );
	    my $result_chil_from_remote = _post_content(
		$data->[$k]->[1],
		'application/x-msgpack; charset=x-user-defined',
		$payload,
		$self->verify_hostname
	       );
	    my $result_with_id;
	    if($result_chil_from_remote->{is_success}){
		my $result_chil = _msgpack_to_perl($result_chil_from_remote->{res});
		$result_with_id = {id => $k, result => $result_chil->{result}, remote => $data->[$k]->[1], is_success => 1};
	    }else{
		$result_with_id = {id => $k, remote => $data->[$k]->[0], is_success => 0, failed_data => $data->[$k]};
	    }
	    $pm->finish(0,$result_with_id);
	}
    }else{
	for(my $k=0; $k <= $#$data; $k++){
	    $pm->start and next;
	    my $result_chil = $mapper_ref->($data->[$k]);
	    my $result_with_id = {id => $k, result => $result_chil, is_success => 1, remote => 'LOCAL'};
	    $pm->finish(0,$result_with_id);
	}
    }
    $pm->wait_all_children;
    my $result_failover;
    if($remote_flg == 1 and $#$failed_data >= 0){
	my @succeeded_remotes_list;
	foreach my $key (keys %$succeeded_remotes){
	    push(@succeeded_remotes_list,$key);
	}
	my $pm2 = Parallel::ForkManager->new($max_proc);
	$pm2->run_on_finish(
	    sub {
		my ($pid, $exit_code, $ident, $exit_signal, $core_dump, $data_structure) = @_;
		if (defined $data_structure) {
		    if($data_structure->{is_success} == 1){
			$succeeded_remotes->{$data_structure->{remote}} = 1;
			$result_failover->[$data_structure->{id}] = $data_structure->{result};
		    }else{
			$failed_remotes->{$data_structure->{remote}} = 1;
			push(@$discarded_data,$data_structure->{failed_data});
			$result_failover->[$data_structure->{id}] = undef;
		    }
		}
	    }
	   );

	for(my $k=0; $k <= $#$failed_data; $k++){
	    $pm2->start and next;
	    my $payload = _perl_to_msgpack(
		{
		    data => $failed_data->[$k]->[0],
		    code => $stringified_code
		   }
	       );
	    my $rand_remote = $succeeded_remotes_list[int(rand($#succeeded_remotes_list))];
	    my $result_chil_from_remote = _post_content(
		$rand_remote,
		'application/x-msgpack; charset=x-user-defined',
		$payload,
		$self->verify_hostname
	       );
	    my $result_with_id;
	    if($result_chil_from_remote->{is_success}){
		my $result_chil = _msgpack_to_perl($result_chil_from_remote->{res});
		$result_with_id = {id => $#$data + $k, result => $result_chil->{result}, remote => $rand_remote, is_success => 1};
	    }else{
		$result_with_id = {id => $#$data + $k, remote => $rand_remote, is_success => 0, failed_data => $failed_data->[$k]};
	    }
	    $pm2->finish(0,$result_with_id);
	}
	$pm2->wait_all_children;
    }
    my $result_merged;
    push(@$result_merged,@$result);
    if($#$result_failover >= 0){
	push(@$result_merged,@$result_failover);
    }
    if($#$discarded_data >= 0){
	if($self->die_discarded_data == 1){
	    die "Fatal: Discarded data exist due to remote server couldn't process requested data.\n";
	}elsif($self->warn_discarded_data == 1){
	    warn "Warning: Discarded data exist.\n";
	}
    }
    if($self->skip_undef_result == 1){
	my $result_skip;
	for(0 .. $#$result_merged){
	    if(defined($result_merged->[$_])){
		push(@$result_skip,$result_merged->[$_]);
	    }
	}
	return($reducer_ref->($result_skip));
    }else{
	return($reducer_ref->($result_merged));
    }
}

sub worker {
    my $self = shift;
    my $path = shift;
    my $worker = shift;
    my $port = shift;
    unless(defined($worker)){
	$worker = 4;
    }
    unless(defined($port)){
	$port = 5000;
    }
    my $rc = eval{
	require Plack::Handler::Starlet;
	1;
    };
    if($rc and $self->force_plackup == 0){
	print "Starting MapReduce Framework Worker by Starlet\n";
	print "Path: $path\nPort: $port\n";
	my $app = $self->load_worker_plack_app($path);
	my $handler = Plack::Handler::Starlet->new(
	    max_workers => $worker,
	    port => $port
	   );
	$handler->run($app);
    }else{
	require Plack::Runner;
	my $runner = Plack::Runner->new;
	print "Starting MapReduce Framework Worker by plackup. The number of workers will be ignored.\n";
	print "Path: $path\nPort: $port\n";
	my $app = $self->load_worker_plack_app($path);
	$runner->parse_options('--port',$port);
	$runner->run($app);
    }
}

sub load_worker_plack_app {
    my $self = shift;
    my $path = shift;
    my $app = sub {
	my $env = shift;
	my $req = Plack::Request->new($env);
	if($self->worker_log == 1){
	    print "REQ,$$,".$req->address.',';
	    my @tar = localtime(time());
	    printf(
		"%04d-%02d-%02d %02d:%02d:%02d",
		$tar[5]+1900,$tar[4]+1,$tar[3],$tar[2],$tar[1],$tar[0]
	       );
	    print "\n";
	}
	my $response = {
	    $path => sub {
		my $msg_req = $req->content //
		    return [400,['Content-Type' => 'text/plain'],['Content body required.']];
		my $perl_req = _msgpack_to_perl($msg_req) //
		    return [400,['Content-Type' => 'text/plain'],['Valid MessagePack required']];
		my $data = $perl_req->{data};
		my $code_text = $perl_req->{code};
		my $code_ref;
		eval('$code_ref = sub '.$code_text.';');
		my $result = $code_ref->($data);
		return [200,['Content-Type' => 'application/x-msgpack; charset=x-user-defined'],[_perl_to_msgpack({result => $result})]];
	    }
	   };
	if($self->worker_log == 1){
	    print "END,$$,".$req->address.',';
	    my @tar = localtime(time());
	    printf(
		"%04d-%02d-%02d %02d:%02d:%02d",
		$tar[5]+1900,$tar[4]+1,$tar[3],$tar[2],$tar[1],$tar[0]
	       );
	    print "\n";
	}
	if(defined($response->{$env->{PATH_INFO}})){
	    return $response->{$env->{PATH_INFO}}->();
	}else{
	    return [404,['Content-Type' => 'text/plain'],['Not Found']];
	}

    };
    return($app);
}


sub _post_content {
    my $url = shift;
    my $content_type = shift;
    my $data = shift;
    my $ssl_opt = shift;
    my $ua = WWW::Mechanize->new(
	ssl_opts => {
	    verify_hostname => $ssl_opt
	   }
       );
    my $is_success = 1;
    eval{
	$ua->post($url,'Content-Type' => $content_type, Content => $data);
    };
    if($@){
	$is_success = 0;
    }
    my $res = $ua->content();
    return {res => $res, is_success => $is_success};
}

sub _perl_to_msgpack {
    my $data = shift;
    my $msgpack = Data::MessagePack->new();
    my $packed = $msgpack->pack($data);
    return($packed);
}

sub _msgpack_to_perl {
    my $msg_text = shift;
    my $msgpack = Data::MessagePack->new();
    my $unpacked = $msgpack->unpack($msg_text);
    return($unpacked);
}



__PACKAGE__->meta->make_immutable();

1;
__END__

=encoding utf-8

=head1 NAME

MapReduce::Framework::Simple - Simple Framework for MapReduce

=head1 SYNOPSIS

    ## After install this module, you can start MapReduce worker server by this command.
    ## $ perl -MMapReduce::Framework::Simple -e 'MapReduce::Framework::Simple->new->worker("/eval");'
    ## Prefork HTTP server module "Starlet" is highly recommended for practical uses.

    use MapReduce::Framework::Simple;
    use Data::Dumper;

    my $mfs = MapReduce::Framework::Simple->new;

    ## Generate data for MapReduce manually.
    my $data_map_reduce;
    for(0 .. 2){
        my $tmp_data;
        for(0 .. 10000){
            push(@$tmp_data,rand(10000));
        }
        # Records should be [[<data>,<worker url>],...]
        push(@$data_map_reduce,[$tmp_data,'http://localhost:5000/eval']);
        # If you want to use standalone, Record should be [<data>] as below
        # push(@$data_map_reduce,$tmp_data);
    }


    ## OR, Generate good balanced data for MapReduce automatically.
    my $remote_servers = [
        'http://remote1.local:5000/eval_secret_url',
        'http://remote2.local:5000/eval_secret_url',
        'http://remote3.local:5000/eval_secret_url'
       ];
    my $tmp_data_2;
    for(0 .. 100000){
        push(@$tmp_data_2,rand(10000));
    }
    my $data_auto_assign = $mfs->create_assigned_data(
        $tmp_data_2,
        $remote_servers,
        { chunk_num => 10, method => 'volume_uniform' }
       );

    # mapper code
    my $mapper = sub {
        my $input = shift;
        my $sum = 0;
        my $num = $#$input + 1;
        for(0 .. $#$input){
            $sum += $input->[$_];
        }
        my $avg = $sum / $num;
        return({avg => $avg, sum => $sum, num => $num});
    };

    # reducer code
    my $reducer = sub {
        my $input = shift;
        my $sum = 0;
        my $avg = 0;
        my $total_num = 0;
        for(0 .. $#$input){
            $sum += $input->[$_]->{sum};
            $total_num += $input->[$_]->{num};
        }
        $avg = $sum / $total_num;
        return({avg => $avg, sum => $sum});
    };

    my $result = $mfs->map_reduce(
        $data_map_reduce,
        $mapper,
        $reducer,
        5
       );

    # Stand alone
    # my $result = $mfs->map_reduce(
    #     $data_map_reduce,
    #     $mapper,
    #     $reducer,
    #     5,
    #     {remote => 0}
    #    );

    print Dumper $result;

=head1 DESCRIPTION

MapReduce::Framework::Simple is simple grid computing framework for MapReduce model.
MapReduce is a better programming model for solving highly parallelizable problems like a word-count from large number of documents.

The model requires Map procedure that processes given data with given sub-routine(code reference) parallelly and Reduce procedure that summarizes outputs from Map sub-routine.

This module provides worker server that just computes perl-code and data sent from remote client.
You can start MapReduce worker server by one liner Perl.
Pre-fork HTTP server module "Starlet" will be loaded automatically if it is installed. Starlet installed environment is highly recommended for practical uses.

=head1 METHODS

=head2 I<new>

I<new> creates object.

    my $mfs->MapReduce::Framework::Simple->new(
        verify_hostname => 1, # verify public key fingerprint.
        skip_undef_result => 1, # skip undefined value at reduce step.
        warn_discarded_data => 1, # warn if discarded data exist due to some connection problems.
        die_discarded_data => 0 # die if discarded data exist.
        worker_log => 0 # print worker log when remote client accesses.
        force_plackup => 0 # force to use plackup when starting worker server.
        );

=head2 I<create_assigned_data>

This method creates MapReduce ready data from data and remote worker server list.
You can set the number of data chunk and balancing method ('volume_uniform','element_shuffle','element_sequential').

    my $tmp_data = [1 .. 1_000_000];
    my $server_list = [
        'http://s1.local:5000/eval',
        'http://s2.local:5000/eval',
        'http://s3.local:5000/eval',
       ];

    my $data = $mfs->create_assigned_data(
        $tmp_data,
        $server_list,
        {
            chunk_num => 10, # number of data chunk.
            method => 'volume_uniform', # balancing method.
           }
       );

=head2 I<map_reduce>

I<map_reduce> method starts MapReduce processing using Parallel::ForkManager.

    my $result = $mfs->map_reduce(
        $data_map_reduce, # data
        $mapper, # code ref of mapper
        $reducer, # code ref of reducer
        5, # number of fork process
        {remote => 1} # grid computing flag.
       );

=head2 I<worker>

I<worker> method starts MapReduce worker server using Starlet HTTP server over Plack when Starlet is installed (or not, startup by single process plack server. It is not for practical uses).
If you need to startup worker as plackup on the environment that has Starlet installed, please set force_plackup => 1 when I<new>.

Warning: Worker server do eval remote code. Please use this server at secure network.

    $mfs->worker(
        "/yoursecret_eval_path", # path
        4, # number of preforked Starlet worker
        5000 # port number
        );

=head2 I<load_worker_plack_app>

If you want to use other HTTP server, you can extract Plack app by I<load_worker_plack_app> method.

    use Plack::Loader;
    my $app = $mfs->load_worker_plack_app("/yoursecret_eval_path");
    my $handler = Plack::Loader->load(
           'YOURFAVORITESERVER',
           ANY => 'FOO'
           );
    $handler->run($app);

Example one liner deploy code below (with Starlight the pure Perl pre-fork HTTP server).

    $ perl -MMapReduce::Framework::Simple -MPlack::Loader -e 'Plack::Loader->load("Starlight", port => 12345)->run(MapReduce::Framework::Simple->new->load_worker_plack_app("/eval_secret"))'


=head1 PERFORMANCE

This methodology is suitable for Highly-Parallelizable problems.

=head2 Example: Summation of prime numbers

Normally, we calculate the summation of prime numbers in 1,000,000,001 to 1,000,300,000 like below.

    use strict;
    use warnings;

    my $num_list = [1_000_000_001 .. 1_000_300_000];
    my $sum=0;
    for(@$num_list){
        my $flag = 0;
        for( my $k=2; $k <= int(sqrt($_)); $k++){
            if(($_ % $k) == 0){
                $flag = 1;
                last;
            }
        }
        if($flag == 0){
            $sum += $_;
        }
    }

    print "$sum\n";

I guess this problem will be solved around 1 minute after execute this program.

Here is parallel processing version of this program by using this module. It might be solved in 10 seconds.

    use strict;
    use warnings;
    use MapReduce::Framework::Simple;

    my $mfs = MapReduce::Framework::Simple->new(
        skip_undef_result => 0,
        warn_discarded_data => 1
       );

    my $server_list = [
        'http://remote1.example.com:5000/eval', # 20 cores over remote server.
        'http://remote2.example.com:5000/eval', # 20 cores over remote server.
       ];

    my $data_tmp;

    my $parallel_num = 10;
    for (1_000_000_001 .. 1_000_300_000){
        push(@$data_tmp,$_);
    }

    my $data = $mfs->create_assigned_data(
        $data_tmp,
        $server_list,
        {
            chunk_num => 40,
            method => 'element_shuffle'
           }
       );

    # mapper code
    my $mapper = sub {
        my $input = shift;
        my $sum=0;
        for(0 .. $#$input){
            my $flag = 0;
            for( my $k=2; $k <= int(sqrt($input->[$_])); $k++){
                if(($input->[$_] % $k) == 0){
                    $flag = 1;
                    last;
                }
            }
            if($flag == 0){
                $sum += $input->[$_];
            }
        }
        return($sum);
    };

    # reducer code
    my $reducer = sub {
        my $input = shift;
        my $sum=0;
        foreach my $tmp_input (@$input){
            $sum += $tmp_input;
        }
        return($sum);
    };

    my $result = $mfs->map_reduce(
        $data,
        $mapper,
        $reducer,
        $parallel_num,
        {remote => 1}
       );

    print "$result\n";


=head1 EFFECTIVENESS

Sometimes we regret things we design the programs and routines that process small data.

Please check the current design when you convert to MapReduce model.

=head2 Is this procedure parallelizable?

The problem that you want to solve should be highly-parallelizable if you convert to MapReduce model.

=head2 Are there data size predictable?

If these data size assined to workers are not predictable, acceleration of computing by converting to MapReduce model can not be expected because each workers has unevenness amount of tasks and actual processing time.

=head2 Is overhead relatively small?

Please read some documents related to "Amdahl's law" and "embarrassingly parallel".


=head1 LICENSE

Copyright (C) Toshiaki Yokoda.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 AUTHOR

Toshiaki Yokoda E<lt>adokoy001@gmail.comE<gt>

=cut

