function [portfolio_info, portfolio_constraint, factor_constraint] = ConfigReaderToday(config_path, useToday, specifiedDate)
% ConfigReaderToday -
%
% 用法:
%   [portfolio_info, portfolio_constraint, factor_constraint] = ConfigReaderToday(config_path)
%   [portfolio_info, portfolio_constraint, factor_constraint] = ConfigReaderToday(config_path, true)
%   [portfolio_info, portfolio_constraint, factor_constraint] = ConfigReaderToday(config_path, false)
%
% 说明:
%   - 本函数会调用现有的 ConfigReader 函数读取配置，然后根据第二个参数
%     决定是否将 portfolio_info 中的 start_date / end_date 覆盖为今天。
%
% 参数:
%   config_path   - 配置文件路径（通常为 fullfile(script_dir, 'config', 'opt_project_config.xlsx')）
%   useToday      - 可选，true/false（默认为 true）
%   specifiedDate - 可选，指定用于覆盖的日期（datetime 或 yyyy-MM-dd 字符串）。
%                   如果提供了本参数，则优先使用该日期覆盖 start_date/end_date，
%                   无论 useToday 为何值。

    if nargin < 2 || isempty(useToday)
        useToday = true;
    end
    if nargin < 3
        specifiedDate = [];
    end


    try
        portfolio_info = readtable(config_path, 'Sheet', 'portfolio_info');
    catch ME
        error('Failed to read portfolio_info sheet from %s: %s', config_path, ME.message);
    end

    try
        opts = detectImportOptions(config_path, 'Sheet', 'portfolio_constraint');
        
        opts = setvartype(opts, opts.VariableNames, 'char');
        portfolio_constraint = readtable(config_path, opts);
    catch
        
        portfolio_constraint = table();
    end

    try
        factor_constraint = readtable(config_path, 'Sheet', 'factor_constraint');
    catch
        factor_constraint = table();
    end

    fprintf('成功读取配置文件 (ConfigReader 内联实现):\n');
    if exist('portfolio_info', 'var') && height(portfolio_info) > 0
        fprintf('  投资组合数量: %d\n', height(portfolio_info));
        fprintf('  投资组合列表:\n');
        for i = 1:height(portfolio_info)
            if ismember('portfolio_name', portfolio_info.Properties.VariableNames)
                pname = portfolio_info.portfolio_name{i};
            else
                pname = sprintf('portfolio_%d', i);
            end
            fprintf('    %d. %s\n', i, pname);
        end
    end

   
    if isempty(specifiedDate) && ~useToday
        return;
    end

    % 解析指定日期或使用今天
    if ~isempty(specifiedDate)
        if isdatetime(specifiedDate)
            override_dt = specifiedDate;
        else
            try
                % 尝试按 yyyy-MM-dd 解析字符串
                override_dt = datetime(char(specifiedDate), 'InputFormat', 'yyyy-MM-dd');
            catch
                % 回退到通用解析
                override_dt = datetime(char(specifiedDate));
            end
        end
    else

        today_dt = datetime('today');

       
        [is_trading, actual_date] = ValidateWorkingDay(today_dt);
      
        now_dt = datetime('now');
        disp(now_dt);
        if is_trading && hour(now_dt) >= 19
            override_dt = NextWorkdayCalculator(today_dt);
        elseif ~is_trading
            last = datetime(actual_date, 'InputFormat', 'yyyy-MM-dd');
            override_dt = NextWorkdayCalculator(last);
        else
            override_dt = today_dt;
        end
    end

    if ismember('start_date', portfolio_info.Properties.VariableNames)
        try
            portfolio_info.start_date(:) = repmat(override_dt, height(portfolio_info), 1);
        catch
            portfolio_info.start_date = repmat(override_dt, height(portfolio_info), 1);
        end
    else
        portfolio_info.start_date = repmat(override_dt, height(portfolio_info), 1);
    end

    if ismember('end_date', portfolio_info.Properties.VariableNames)
        try
            portfolio_info.end_date(:) = repmat(override_dt, height(portfolio_info), 1);
        catch
            portfolio_info.end_date = repmat(override_dt, height(portfolio_info), 1);
        end
    else
        portfolio_info.end_date = repmat(override_dt, height(portfolio_info), 1);
    end

end
