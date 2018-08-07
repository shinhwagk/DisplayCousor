import sys
import argparse
import os

import cx_Oracle

# select username from v$session group by username;
sqlidSQL = """SELECT
    id,
    operation,
    depth,
    options,
    object_name,
    cardinality,
    bytes,
    cost,
    cpu_cost,
    io_cost,
    access_predicates,
    filter_predicates,
    time,
    plan_hash_value，
    partition_start,
    partition_stop
FROM v$sql_plan
WHERE sql_id = :sql_id AND child_number = :child_number
ORDER BY id, parent_id"""


sqlTextSQL = "SELECT sql_text FROM v$sqltext WHERE sql_id = :sql_id ORDER BY piece"


def noneToEmptyStr(v):
    return '' if v is None else v


def query(connect, sql, **kps):
    c = connect.cursor()
    c.execute(sql, **kps)
    rs = [[v for v in list(r)] for r in c.fetchall()]  # row set
    if len(rs) >= 1:
        cns = [c[0] for c in c.description]  # colume name set
        rs.insert(0, cns)
    c.close()
    return rs


def query_sql_full_text(connect, sql_id):
    return query(connect, sqlTextSQL, sql_id=sql_id)


def query_xplan_by_sql_id(connect, sql_id, child_number):
    return query(connect, sqlidSQL, sql_id=sql_id, child_number=child_number)


def get_colume_name_index(rs, cn):
    for idx, val in list(enumerate(rs[0])):
        if val == cn:
            return idx


def get_colume_name_colume(rs, n):
    i = get_colume_name_index(rs, n)
    return [r[i] for r in rs]


def format_sp_id(rs):
    """format sql plan"""
    c_id_v = get_colume_name_colume(rs, "ID")[1:]
    c_fp_v = get_colume_name_colume(rs, "FILTER_PREDICATES")[1:]
    c_ap_v = get_colume_name_colume(rs, "ACCESS_PREDICATES")[1:]
    c_id_max_len = len(str(max(c_id_v)))
    c_id = []
    for i, [fp_v, ap_v] in list(enumerate(zip(c_fp_v, c_ap_v))):
        if fp_v is None and ap_v is None:
            c_id.append(str(c_id_v[i]).rjust(c_id_max_len + 3) + ' ')
        else:
            c_id.append('*' + str(c_id_v[i]).rjust(c_id_max_len + 2) + ' ')
    c_max_len = max([len(v) for v in c_id])
    c_id.insert(0, ' ID'.ljust(c_max_len))
    return c_id


def format_sp_operation(rs):
    """format sql plan"""
    c_ot_v = [noneToEmptyStr(v)
              for v in get_colume_name_colume(rs, "OPERATION")[1:]]
    c_o_v = [noneToEmptyStr(v)
             for v in get_colume_name_colume(rs, "OPTIONS")[1:]]
    c_d_v = [noneToEmptyStr(v)
             for v in get_colume_name_colume(rs, "DEPTH")[1:]]
    n_operation = [' '.join(i) if len(i[1]) >= 1 else i[0]
                   for i in zip(c_ot_v, c_o_v)]
    n_d_operation = [''.join([' ']*d) + o for o, d in zip(n_operation, c_d_v)]
    n_d_operation.insert(0, 'OPERATION')
    return format_sp_align(n_d_operation, 1)


def format_sp_cost(rs):
    """format sql plan"""
    c_c_v = get_colume_name_colume(rs, "COST")[1:]
    c_ic_v = get_colume_name_colume(rs, "IO_COST")[1:]
    r = []
    l = []
    for _, [c, ic] in list(enumerate(zip(c_c_v, c_ic_v))):
        n_ic = 0 if ic is None else ic
        n_c = 0 if c is None else c
        x = 0 if n_c == 0 or n_ic == 0 else (n_c - n_ic) / n_c * 100
        r.append("(%d)" % x)
        l.append(n_c)
    r_max_len = len(max(r))
    cost = ["%s %s" % (str(lv), rv.rjust(r_max_len)) for lv, rv in zip(l, r)]
    cost.insert(0, "COST (%CPU)")
    return format_sp_align(cost, 0)


def format_sp_time(rs):
    """format sql plan"""
    t = get_colume_name_colume(rs, "TIME")[1:]
    n_time = []
    for tv in t:
        if tv is not None:
            hours = tv // 3600
            temp = tv - 3600 * hours
            minutes = temp // 60
            seconds = temp - 60 * minutes
            n_time.append('%02d:%02d:%02d' % (hours, minutes, seconds))
        else:
            n_time.append('')
    n_time.insert(0, "TIME")
    return format_sp_align(n_time, 1)


def format_sp_align(c, i):
    """format sql plan"""
    """i=0 rigth,i=1 left"""
    c_max_len = max([len(v) for v in c])
    cn = (" " + c[0]).ljust(c_max_len+2)
    if i == 0:
        return [cn] + [" " + v.rjust(c_max_len) + " " for v in c[1:]]
    else:
        return [cn] + [" " + v.ljust(c_max_len) + " " for v in c[1:]]


def format_sp_table(rs, plan_hash_value):
    """ format sql plan"""
    trs = []
    trs.append("Plan hash value: %s" % plan_hash_value)
    trs.append('')
    tl = sum([len(v) for v in rs[0]]) + len(rs[0]) + 1
    trs.append(''.rjust(tl, '-'))
    trs.append("|%s|" % "|".join(rs[0]))
    trs.append(''.rjust(tl, '-'))
    for r in rs[1:]:
        trs.append("|"+"|".join(r)+"|")
    trs.append(''.rjust(tl, '-'))
    return trs


def format_sp_name(rs):
    """format sql plan"""
    n_name = [noneToEmptyStr(v)
              for v in get_colume_name_colume(rs, "OBJECT_NAME")]
    n_name[0] = "NAME"
    return format_sp_align(n_name, 1)


def format_sp_part(rs):
    """format sql plan"""
    pst = get_colume_name_colume(rs, "PARTITION_START")[1:]
    psp = get_colume_name_colume(rs, "PARTITION_STOP")[1:]
    n_pst = []
    n_psp = []
    is_part = False
    for [t, p] in zip(pst, psp):
        if t is None:
            n_pst.append('')
            n_psp.append('')
        else:
            is_part = True
            n_pst.append(str(t))
            n_psp.append(str(p))
    if is_part:
        t = format_sp_align(n_pst, 1)
        t.insert(0, 'PSTART')
        p = format_sp_align(n_psp, 1)
        p.insert(0, 'PSTOP')
        return is_part, t, p
    else:
        return is_part, None, None


def format_sp_rows(rs):
    """format sql plan"""
    n_rows = []
    afr = False
    afr_arr = []
    for i, v in list(enumerate(get_colume_name_colume(rs, "CARDINALITY")[1:])):
        if v is None:
            n_rows.append('')
        else:
            if v >= 10000:
                afr = True
                if v >= 1000 * 1000 * 1000:
                    n_rows.append("%.1fG" % v/(1000 * 1000 * 1000))
                elif v >= 1000 * 1000:
                    n_rows.append("%dM" % v//(1000 * 1000))
                else:
                    n_rows.append("%dK" % (v//1000))
            else:
                n_rows.append(str(v))
                afr_arr.append(i)
    if afr:
        for i in afr_arr:
            n_rows[i] = n_rows[i] + ' '
    n_rows.insert(0, "ROWS")
    return format_sp_align(n_rows, 0)


def format_sp_bytes(rs):
    """format sql plan"""
    n_bytes = []
    afr = False
    afr_arr = []
    for i, v in list(enumerate(get_colume_name_colume(rs, "BYTES")[1:])):
        if v is None:
            n_bytes.append('')
        else:
            if v >= 10000:
                afr = True
                if v >= 1024 * 1024 * 1024:
                    n_bytes.append("%.1fG" % (v/(1024 * 1024 * 1024)))
                elif v >= 1024 * 1024:
                    n_bytes.append("%dM" % (v//(1024 * 1024)))
                else:
                    n_bytes.append("%dK" % (v//1024))
            else:
                n_bytes.append(str(v))
                afr_arr.append(i)
    if afr:
        for i in afr_arr:
            n_bytes[i] = n_bytes[i] + ' '
    n_bytes.insert(0, "BYTES")
    return format_sp_align(n_bytes, 0)


def format_sp_combine(rs):
    """format sql plan"""
    n_id = format_sp_id(rs)
    n_operation = format_sp_operation(rs)
    n_cost = format_sp_cost(rs)
    n_name = format_sp_name(rs)
    n_rows = format_sp_rows(rs)
    n_bytes = format_sp_bytes(rs)
    n_time = format_sp_time(rs)
    is_part, n_part_t, n_part_p = format_sp_part(rs)
    if is_part:
        return list(zip(n_id, n_operation, n_name, n_rows, n_bytes, n_cost, n_time, n_part_t, n_part_p))
    else:
        return list(zip(n_id, n_operation, n_name, n_rows, n_bytes, n_cost, n_time))


def format_sp(rs, sql_id, child_number):
    phv = get_colume_name_colume(rs, 'PLAN_HASH_VALUE')[1]
    sc = format_sp_combine(rs)
    return format_sp_table(sc, phv)


def format_st(c, sql_id, child_number):
    "SQL_ID  sql_id, child number child_number"
    tit = "SQL_ID  %s, child number %s" % (sql_id, child_number)
    tit_ = ''.rjust(len(tit), '-')
    return [tit, tit_] + [t[0] for t in query_sql_full_text(c, sql_id)[1:]]


def format_qbn():
    "Query Block Name / Object Alias (identified by operation id):"
    return []


def format_pi(rs):
    "Predicate Information (identified by operation id):"
    c_i_v = get_colume_name_colume(rs, "ID")[1:]
    c_fp_v = get_colume_name_colume(rs, "FILTER_PREDICATES")[1:]
    c_ap_v = get_colume_name_colume(rs, "ACCESS_PREDICATES")[1:]
    t_pi = []
    for i, [fp, ap] in list(enumerate(zip(c_fp_v, c_ap_v))):
        if fp is not None:
            t_pi.append([c_i_v[i], "filter", fp])
        if ap is not None:
            t_pi.append([c_i_v[i], "access", ap])
    tit = "Predicate Information (identified by operation id):"
    tit_ = ''.rjust(len(tit), '-')

    pi = []
    for i, x, fa in t_pi:
        pi.append("   %d - %s(%s)" % (i, x, fa))

    return [tit, tit_, ''] + pi


def format_cpi():
    "Column Projection Information (identified by operation id):"
    return []


def parse_args():
    return ""


def dc_main(dsn, sql_id, child_number):
    c = cx_Oracle.connect(dsn)
    rs = query_xplan_by_sql_id(c, sql_id, child_number)
    sp = format_sp(rs, sql_id, child_number)
    st = format_st(c, sql_id, child_number)
    pi = format_pi(rs)
    c.close()
    return st + [''] + sp + [''] + pi


class display_cursor:
    def __init__(self, dsn, sql_id, child_number):
        self.__sp = dc_main(dsn, sql_id, child_number)

    def print(self):
        for l in self.__sp:
            print(l)

    def to_str(self):
        return "\n".join(self.__sp)

    def str_lines(self):
        return self.__sp
