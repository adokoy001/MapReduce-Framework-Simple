package MapReduce::Framework::Simple;
use 5.008001;
use strict;
use warnings;
use B::Deparse;
use Mouse;
use Data::MessagePack;
use Parallel::ForkManager;
use Plack::Request;
use Plack::Handler::Starlet;
use WWW::Mechanize;

has 'verify_hostname' => (is => 'rw', isa => 'Int', default => 1);

our $VERSION = "0.01";

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
    my $result;
    my $pm = Parallel::ForkManager->new($max_proc);
    $pm->run_on_finish(
	sub {
	    my ($pid, $exit_code, $ident, $exit_signal, $core_dump, $data_structure) = @_;
	    if (defined $data_structure) {
		$result->[$data_structure->{id}] = $data_structure->{result};
	    }
	}
       );
    if($remote_flg == 1){
	for(my $k=0; $k <= $#$data; $k++){
	    $pm->start and next;
	    my $stringified_code = B::Deparse->new->coderef2text($mapper_ref);
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
	    my $result_chil = _msgpack_to_perl($result_chil_from_remote);
	    my $result_with_id = {id => $k, result => $result_chil->{result}};
	    $pm->finish(0,$result_with_id);
	}
    }else{
	for(my $k=0; $k <= $#$data; $k++){
	    $pm->start and next;
	    my $result_chil = $mapper_ref->($data->[$k]);
	    my $result_with_id = {id => $k, result => $result_chil};
	    $pm->finish(0,$result_with_id);
	}
    }
    $pm->wait_all_children;
    return($reducer_ref->($result));
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
    print "Starting MapReduce Framework Worker by Starlet\n";
    print "Path: $path\nPort: $port\n";
    my $app = $self->load_worker_plack_app($path);
    my $handler = Plack::Handler::Starlet->new(
	max_worker => $worker,
	port => $port
       );
    $handler->run($app);
}

sub load_worker_plack_app {
    my $self = shift;
    my $path = shift;
    my $app = sub {
	my $env = shift;
	my $req = Plack::Request->new($env);
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
    $ua->post($url,'Content-Type' => $content_type, Content => $data);
    my $res = $ua->content();
    return $res;
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
    use MapReduce::Framework::Simple;
    use Data::Dumper;

    my $mfs = MapReduce::Framework::Simple->new;

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

    # mapper code
    my $mapper = sub {
        my $input = shift;
        my $sum = 0;
        my $num = $#$input + 1;
        for(0 .. $#$input){
            $sum += $input->[$_];
        }
        my $avg = $sum / $num;
        return({avg => $avg, sum => $sum});
    };

    # reducer code
    my $reducer = sub {
        my $input = shift;
        my $sum = 0;
        my $avg = 0;
        my $num = $#$input + 1;
        for(0 .. $#$input){
            $sum += $input->[$_]->{sum};
            $avg += $input->[$_]->{avg};
        }
        $avg = $avg / $num;
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

You can start MapReduce worker server by one liner Perl.

=head1 METHODS

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

I<worker> method starts MapReduce worker server using Starlet HTTP server over Plack.

Warning: Worker server do eval remote code. Please use this server at secure network.

    $mfs->worker(
        "/yoursecret_eval_path", # path
        4, # number of preforked Starlet worker
        5000 # port number
        );

=head2 I<load_worker_plack_app>

If you want to use other HTTP server, you can extract Plack app by I<load_worker_plack_app> method

    use Plack::Loader;
    my $app = $mfs->load_worker_plack_app("/yoursecret_eval_path");
    my $handler = Plack::Loader->load(
           'YOURFAVORITESERVER',
           ANY => 'FOO'
           );
    $handler->run($app);

=head1 LICENSE

Copyright (C) Toshiaki Yokoda.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 AUTHOR

Toshiaki Yokoda E<lt>adokoy001@gmail.comE<gt>

=cut

