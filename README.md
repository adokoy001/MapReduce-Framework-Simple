[![Build Status](https://travis-ci.org/adokoy001/MapReduce-Framework-Simple.svg?branch=master)](https://travis-ci.org/adokoy001/MapReduce-Framework-Simple)
# NAME

MapReduce::Framework::Simple - Simple Framework for MapReduce

# SYNOPSIS

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

# DESCRIPTION

MapReduce::Framework::Simple is simple grid computing framework for MapReduce model.

You can start MapReduce worker server by one liner Perl.

# METHODS

## _new_

_new_ creates object.

    my $mfs->MapReduce::Framework::Simple->new(
        verify_hostname => 1, # verify public key fingerprint.
        skip_undef_result => 1, # skip undefined value at reduce step.
        warn_discarded_data => 1, # warn if discarded data exist due to some connection problems.
        die_discarded_data => 0 # die if discarded data exist.
        );

## _map\_reduce_

_map\_reduce_ method starts MapReduce processing using Parallel::ForkManager.

    my $result = $mfs->map_reduce(
        $data_map_reduce, # data
        $mapper, # code ref of mapper
        $reducer, # code ref of reducer
        5, # number of fork process
        {remote => 1} # grid computing flag.
       );

## _worker_

_worker_ method starts MapReduce worker server using Starlet HTTP server over Plack.

Warning: Worker server do eval remote code. Please use this server at secure network.

    $mfs->worker(
        "/yoursecret_eval_path", # path
        4, # number of preforked Starlet worker
        5000 # port number
        );

## _load\_worker\_plack\_app_

If you want to use other HTTP server, you can extract Plack app by _load\_worker\_plack\_app_ method

    use Plack::Loader;
    my $app = $mfs->load_worker_plack_app("/yoursecret_eval_path");
    my $handler = Plack::Loader->load(
           'YOURFAVORITESERVER',
           ANY => 'FOO'
           );
    $handler->run($app);

# LICENSE

Copyright (C) Toshiaki Yokoda.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

# AUTHOR

Toshiaki Yokoda <adokoy001@gmail.com>
