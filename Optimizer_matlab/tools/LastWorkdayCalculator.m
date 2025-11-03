function last_workday = LastWorkdayCalculator(target_date)
    % 计算上一个工作日

    config_path = fullfile(fileparts(fileparts(mfilename('fullpath'))), 'config', 'config_db.m');
    run(config_path);

    host = db_config.host;
    dbname = db_config.dbname;
    username = db_config.username;
    password = db_config.password;
    conn = database(dbname, username, password, ...
                    'com.mysql.cj.jdbc.Driver', ...
                    ['jdbc:mysql://' host '/' dbname]);

    if isdatetime(target_date)
        target_date_str = string(target_date, 'yyyy-MM-dd');
    elseif ischar(target_date) || isstring(target_date)
        target_date_str = char(target_date);
    end
    
    query = ['SELECT valuation_date FROM data_prepared_new.chinesevaluationdate ' ...
             'WHERE valuation_date < ''', target_date_str, ''' ' ...
             'ORDER BY valuation_date DESC LIMIT 1'];

    result = fetch(conn, query);
    close(conn);

    last_workday = string(result.valuation_date(1));
    
end
