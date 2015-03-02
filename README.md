# parse wim status

A perl program that uses other calvad libs to do most of the heavy
lifting.

Call with a path to files, and with DB user, host, as in

```
perl -w  parse_wim_status.pl -u slash -host 192.168.0.1 -path t/files/
```

One caveat that I haven't bothered to fix yet.  The current default
file matching pattern will fall down if you have the word "status" in
the passed in path.  So if you pass in

```
-path /my/path/to/status/files/
```

Then every file that has xls or xlsx ending in that directory or any
of its subdirectories will be included.  The only problem is that you
will see a lot of error messages. It shouldn't crash.

Normally, what it does it look for a file name with "status" and an
ending of "xls" or "xlsx".

# Real usage

So what I do is to run this twice.  The first time through I *do not*
substitute undefined for empty status values.  This is because perhaps
the next month's spreadsheet has an updated or corrected value for
this month that will be caught in the "lookback" iteration.

So, the first pass my command line looks like:

```
perl -w  parse_wim_status.pl -u slash -host 192.168.0.1 -path /home/james/Downloads/xlsfiles/ -pattern "\/(ird|pat).*2013.*status.*\.xlsx?$" > process_2013.txt 2>&1 &
```

Note that the output of the script is dumped to the file "process_2013.txt", as well as the error output (that's what the 2>&1 stuff means).

Next, in a second pass, I set the `--write_undefined` flag to make
sure than any empty string values or the odd "?" are written to the
database as "UNDEFINED" status.

```
perl -w  parse_wim_status.pl -u slash -host 192.168.0.1 -path /home/james/Downloads/xlsfiles/ -pattern "\/(ird|pat).*2013.*status.*\.xlsx?$" --write_undefined >> process_2013.txt 2>&1 &
```

Here notice that the output of the script is *appended* to the prior
run's output in the file "process_2013.txt".  That is what the double
right angle caret means (>>).
