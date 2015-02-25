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
