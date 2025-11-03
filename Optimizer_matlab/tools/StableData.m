function [df_st, df_stockuniverse] = StableData()

    config_path = fullfile(fileparts(fileparts(mfilename('fullpath'))), 'config', 'config_db.m');
    run(config_path);

    host = db_config.host;
    dbname = db_config.dbname;
    username = db_config.username;
    password = db_config.password;
    conn = database(dbname, username, password, ...
                    'com.mysql.cj.jdbc.Driver', ...
                    ['jdbc:mysql://' host '/' dbname]);    
    query1 = 'SELECT * FROM data_prepared_new.st_stock';
    query2 = 'SELECT * FROM data_prepared_new.stockuniverse WHERE type = ''stockuni_new''';

    df_st = fetch(conn, query1);
    df_stockuniverse = fetch(conn, query2);
    
    

    close(conn);
end