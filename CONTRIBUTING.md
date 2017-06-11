How To Contribute
===============================================================================


Submit a patch
------------------------------------------------------------------------------

Before sending a Pull Request, please run the ``dockertest.sh`` script to check
that your code works on every current PostgreSQL major versions. For instance,
the command below will check that your version runs correctly on Postgres 9.4:

```
./dockertest.sh 9.4
```

