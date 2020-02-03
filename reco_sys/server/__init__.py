import happybase

pool = happybase.ConnectionPool(size=10, host='hadoop-master', port=9090)
