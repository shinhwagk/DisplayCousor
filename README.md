> similar to oracle dbms package: dbms_xplan.display_cursor.

### install
```sh
pip install xplan
```

#### command line tools
```sh
python -m display_cursor -dsn username/password@ip:port/service_name -sql_id xxx -child_number 0
```

#### simple api
```py3
from xplan import display_cursor

dc = display_cursor(dns,sql_id,child_number)
dc.print()     # console print
dc.to_str()    # str
dc.str_lines() # str array
```
