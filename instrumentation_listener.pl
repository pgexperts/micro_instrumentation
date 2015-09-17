#!/usr/bin/perl


=comment


listener for functime notifications, takes the notifications,
matches up the times so each event has a start and end, and stashes them
in a database table.

Notifications are in the channel "functime" and the payload has the form

   function_name|series_id|timestamp|event

event can be any arbitrary text (except "end").
series_id identifies the instance id of the function being timed

This requires the following table:

    CREATE TABLE func_times(
        pid        int,
        funcname   text,
        series_id  bigint,
        funcevent  text,
        start_time timestamptz,
        end_time   timestamptz
    );

You probably also want:

    ALTER TABLE func_times ADD CONSTRAINT func_times_pk
        PRIMARY KEY (funcname, funcevent, series_id);

For the series_id the caller should use a sequence created like this:

    CREATE SEQUENCE instrument_timing_seq;

Each timed function must get the next value of the sequence and then contain
two or more notifications, the last of which must have the event text set to
"end". In plpgsql, the code would look like this:

    declare
        thisprocseq  bigint := nextval('instrument_timing_seq');
        func_name text = 'name_of_this_function';
    begin
        perform pg_notify('functime',func_name || '|' || thisprocseq || '|'
                          || timeofday() || '|' || 'stage 1');
        --- code for first timed stage
        perform pg_notify('functime',func_name || '|' || thisprocseq || '|'
                          || timeofday() || '|' || 'stage 2');
        -- code for second timed stage
        -- ...
        perform pg_notify('functime',func_name || '|' || thisprocseq || '|'
                          || timeofday() || '|' || 'end');
    end;

=cut


use DBI;
use DBD::Pg;
use Time::HiRes qw( usleep );
use strict;

# either connect using  a SERVICE definition or setting PGUSER, PGDATABASE etc
# in the environment. See Postgres docco for details.

# sets name in pg_stat_activity
$ENV{PGAPPNAME}="functime listener";


my $dbh = DBI->connect("dbi:Pg:", '', '',
					 {AutoCommit => 0, RaiseError => 1}) || die ;

$dbh->do("LISTEN functime");
$dbh->commit;

my $stmt = q{INSERT INTO func_times
    (pid,funcname, series_id, funcevent, start_time, end_time)
    values (?,?,?,?,?,?)};
my $sth = $dbh->prepare($stmt);
my ($s_pid,$s_func,$s_series,$s_evname,$s_start);

## Hang around until we get the message we want
LISTENLOOP:
{
	while (my $notify = $dbh->pg_notifies)
	{
		my ($name, $pid, $payload) = @$notify;
		my ($funcname, $series_id, $ntime, $funcevent) =
		  split(/[|]/,$payload);
		if($s_pid)
		{
			die "invalid event $s_pid <> $pid" unless $s_pid == $pid;
			die "invalid event $s_func <> $funcname" unless $s_func eq $funcname;
			die "invalid event $s_series <> $series_id"
			  unless $s_series == $series_id;
			# event name can and should be different
			$sth->execute($s_pid,$s_func,$s_series,$s_evname,$s_start,$ntime);
		}
		if ($funcevent eq 'end')
		{
			($s_pid,$s_func,$s_series,$s_evname,$s_start) =
			  (undef,undef,undef,undef,undef);
		}
		else
		{
			($s_pid,$s_func,$s_series,$s_evname,$s_start) =
			  ($pid,$funcname,$series_id,$funcevent,$ntime);
		}
	}
	$dbh->ping() or die qq{Ping failed!};
	$dbh->commit();
	usleep(2000000); # 2s
	redo;
}
