> similar to oracle dbms package: dbms_xplan.display_cursor.

### install
pip install display_cursor

#### command line tools
python -m display_cursor -dsn username/password@ip:port/service_name -sql_id xxx -child_number 0 -[print|file]

#### simple api
```py3
import display_cursor

dc = display_cursor(dns,sql_id,child_number)
dc.print()     # console print
dc.to_str()    # str
dc.str_lines() $ str array
```
