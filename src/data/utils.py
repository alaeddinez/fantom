import pkgutil


def read_sql(path):
    """ read a .sql file and return the query
    Parameters
    ----------
        path: string
            path to the .sql file
    Return
    ------
        sql: query inside the .sql file
    """
    #path = "src"+f"/data/{path!s}"
    path = ""+f"{path!s}"
    return open(path, 'r').read()
