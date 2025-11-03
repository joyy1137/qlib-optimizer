function [portfolio_net_value, portfolio_dates, turnover_data, portfolio_returns] = calculate_portfolio_net_value_unified(dbc, index_type, start_date, end_date, data_source, cost_rate)
    % 统一计算投资组合净值（包含换手率成本）
    % 输入:
    %   dbc - 数据库连接器
    %   index_type - 指数类型
    %   start_date - 开始日期
    %   end_date - 结束日期
    %   data_source - 数据源，可以是：
    %                 - 结构体：包含df_components字段（直接传入权重数据）
    %                 - 字符串：包含inputpath_backtesting字段（从文件读取）
    %   cost_rate - 成本费率
    % 输出:
    %   portfolio_net_value - 投资组合净值序列
    %   portfolio_dates - 对应的日期序列
    %   turnover_data - 换手率数据
    %   portfolio_returns - 投资组合收益率序列（扣除换手成本后）
    
    fprintf('\n开始计算投资组合净值...\n');
    fprintf('指数类型: %s\n', index_type);
    fprintf('成本费率: %.4f%% \n', cost_rate*100);
    
    % 判断数据源类型
    if isstruct(data_source) && isfield(data_source, 'df_components')
        % 使用直接传入的权重数据
        use_file_data = false;
        df_components = data_source.df_components;
        fprintf('使用直接传入的投资组合权重数据\n');
    elseif ischar(data_source) || isstring(data_source)
        % 从文件读取数据
        use_file_data = true;
        inputpath_backtesting = char(data_source);
        fprintf('数据路径: %s\n', inputpath_backtesting);
    else
        error('data_source参数格式不正确，应为包含df_components字段的结构体或文件路径字符串');
    end
    
    % 获取股票收益率数据
    df_stock_return = BacktestToolbox.stock_return_withdraw(start_date, end_date);
    
    if isempty(df_stock_return)
        error('未获取到股票收益率数据');
    end
    
    % 根据数据源类型获取日期列表和权重数据
    if use_file_data
        % 从文件系统获取日期列表
        [date_list, daily_weights] = get_dates_and_weights_from_files(inputpath_backtesting);
    else
        % 从股票收益率数据获取日期列表
        [date_list, daily_weights] = get_dates_and_weights_from_database(dbc, index_type, df_stock_return, df_components);
    end
    
    if isempty(date_list)
        error('未找到有效的投资组合数据');
    end
    
    fprintf('找到 %d 个日期的投资组合数据\n', length(date_list));
    
    % 计算投资组合收益率和换手率
    [portfolio_returns, turnover_rates] = calculate_returns_and_turnover(df_stock_return, date_list, daily_weights);
    
    
    if isempty(portfolio_returns) || isempty(turnover_rates)
        error('未成功计算任何投资组合数据');
    end
    
    % 计算投资组合净值
    [portfolio_net_value, portfolio_dates] = calculate_net_value(portfolio_returns, turnover_rates, date_list, cost_rate);
    
    % 确保数据长度一致
    min_length = min([length(portfolio_dates), length(turnover_rates), length(portfolio_net_value)]);

    
    portfolio_dates = portfolio_dates(1:min_length);
    turnover_rates = turnover_rates(1:min_length);
    portfolio_net_value = portfolio_net_value(1:min_length);
    
    % 创建换手率数据表格
    % 确保turnover_rates是列向量
    if size(turnover_rates, 1) == 1
        turnover_rates = turnover_rates';
    end
    
  
    % 确保数据长度一致
    if length(portfolio_dates) ~= length(turnover_rates)
        % fprintf('警告：数据长度不匹配，将截断到最小长度\n');
        min_len = min(length(portfolio_dates), length(turnover_rates));
        portfolio_dates = portfolio_dates(1:min_len);
        turnover_rates = turnover_rates(1:min_len);
        portfolio_net_value = portfolio_net_value(1:min_len);
    end
    
    % 确保turnover_rates是列向量
    if size(turnover_rates, 1) == 1 && size(turnover_rates, 2) > 1
        turnover_rates = turnover_rates';
    end
    
    % 确保portfolio_dates是列向量
    if size(portfolio_dates, 1) == 1 && size(portfolio_dates, 2) > 1
        portfolio_dates = portfolio_dates';
    end
    
    
    % 检查数据长度是否一致
    if length(portfolio_dates) ~= length(turnover_rates)
        error('所有表变量必须具有相同的行数');
    end
    
    turnover_data = table(portfolio_dates, turnover_rates, ...
                         'VariableNames', {'valuation_date', 'turnover_rate'});
    
    % 确保portfolio_net_value是列向量
    if size(portfolio_net_value, 1) == 1 && size(portfolio_net_value, 2) > 1
        portfolio_net_value = portfolio_net_value';
    end
    
    % 保存结果到工作空间
    assignin('base', 'portfolio_result', table(portfolio_dates, portfolio_net_value, ...
                                              'VariableNames', {'valuation_date', 'portfolio_net_value'}));
    
    fprintf('投资组合净值计算完成，结果已保存到 portfolio_result 变量\n');
end

function [date_list, daily_weights] = get_dates_and_weights_from_files(inputpath_backtesting)
    % 从文件系统获取日期列表和权重数据
    
    % 获取可用的日期列表
    available_dates = dir(inputpath_backtesting);
    % 修复数据类型问题
    dir_mask = [available_dates.isdir];
    name_mask = ~strcmp({available_dates.name}, '.') & ~strcmp({available_dates.name}, '..');
    available_dates = available_dates(dir_mask & name_mask);
    date_list = {available_dates.name};
    
    % 过滤出符合日期格式 (yyyy-MM-dd) 的目录
    valid_date_list = {};
    for i = 1:length(date_list)
        dir_name = date_list{i};
        % 检查目录名是否符合日期格式 (yyyy-MM-dd)
        if length(dir_name) == 10 && ...
           dir_name(5) == '-' && dir_name(8) == '-' && ...
           all(isstrprop(dir_name([1:4, 6:7, 9:10]), 'digit'))
            valid_date_list{end+1} = dir_name;
        else
            fprintf('跳过非日期格式目录: %s\n', dir_name);
        end
    end
    
    date_list = sort(valid_date_list);
    fprintf('找到的有效日期目录: %s\n', strjoin(date_list, ', '));
    
    % 获取每日权重数据
    daily_weights = {};
    for i = 1:length(date_list)
        current_date_str = date_list{i};
        current_date = datetime(current_date_str, 'InputFormat', 'yyyy-MM-dd');
        
        try
            [portfolio_weights, ~] = BacktestToolbox.get_portfolio_weights(current_date, inputpath_backtesting);
            if ~isempty(portfolio_weights)
                daily_weights{end+1} = portfolio_weights;
            end
        catch ME
            fprintf('处理日期 %s 时出错: %s\n', current_date_str, ME.message);
            fprintf('跳过该日期，继续处理下一个日期\n');
            continue;
        end
    end
end

function [date_list, daily_weights] = get_dates_and_weights_from_database(dbc, index_type, df_stock_return, df_components)
    % 从数据库获取日期列表和权重数据
    
    % 获取成分股代码列表
    component_codes = df_components.code;
    component_weights = df_components.weight;
    
    % 筛选成分股的收益率数据
    stock_data = df_stock_return(ismember(df_stock_return.code, component_codes), :);
    
    if isempty(stock_data)
        error('未找到成分股的收益率数据');
    end
    
    % 按日期分组
    unique_dates = unique(stock_data.valuation_date);
    unique_dates = sort(unique_dates);
    date_list = unique_dates;
    
    % 获取每日的投资组合权重数据
    daily_weights = cell(length(unique_dates), 1);
    
    for i = 1:length(unique_dates)
        current_date = unique_dates(i);
        
        % 获取当日投资组合权重
        try
            daily_components = dbc.index_component_withdraw(current_date, index_type);
            if ~isempty(daily_components)
                daily_weights{i} = daily_components;
                fprintf('警告：当前使用指数权重作为投资组合权重，实际应用中需要替换为真实的投资组合权重\n');
            else
                if i == 1
                    daily_weights{i} = df_components;
                else
                    daily_weights{i} = daily_weights{i-1};
                end
            end
        catch
            if i == 1
                daily_weights{i} = df_components;
            else
                daily_weights{i} = daily_weights{i-1};
            end
        end
    end
end

function [portfolio_returns, turnover_rates] = calculate_returns_and_turnover(df_stock_return, date_list, daily_weights)
    % 计算投资组合收益率和换手率
    
    portfolio_returns = [];
    turnover_rates = [];
    
    fprintf('开始计算每日投资组合收益率和换手率...\n');
    
    for i = 1:length(date_list)
        current_date = date_list{i};
        
        % 计算当日投资组合收益率
        % 确保数据类型匹配
        if iscell(df_stock_return.valuation_date)
            daily_data = df_stock_return(strcmp(df_stock_return.valuation_date, current_date), :);
        else
            daily_data = df_stock_return(df_stock_return.valuation_date == current_date, :);
        end
        
        % 只有当权重数据存在时才处理
        if ~isempty(daily_weights{i})
            % 计算当日投资组合收益率 - 使用前一天的权重（匹配Python逻辑）
            if ~isempty(daily_data) && i > 1 && ~isempty(daily_weights{i-1})
                % 使用前一天的权重计算当天的收益率
                daily_data_with_weight = innerjoin(daily_data, daily_weights{i-1}, 'Keys', 'code');
                
                if ~isempty(daily_data_with_weight)
                    weighted_returns = daily_data_with_weight.pct_chg .* daily_data_with_weight.weight;
                    daily_return = sum(weighted_returns);
                    portfolio_returns(end+1) = daily_return;
                else
                    portfolio_returns(end+1) = 0;
                end
            elseif i == 1
                % 第一天使用当天的权重
                daily_data_with_weight = innerjoin(daily_data, daily_weights{i}, 'Keys', 'code');
                
                if ~isempty(daily_data_with_weight)
                    weighted_returns = daily_data_with_weight.pct_chg .* daily_data_with_weight.weight;
                    daily_return = sum(weighted_returns);
                    portfolio_returns(end+1) = daily_return;
                else
                    portfolio_returns(end+1) = 0;
                end
            else
                portfolio_returns(end+1) = 0;
            end
            
            % 计算换手率
            if i == 1
                turnover_rates(end+1) = 0;
            else
                turnover_rate = BacktestToolbox.calculate_turnover_rate(daily_weights{i-1}, daily_weights{i});
                turnover_rates(end+1) = turnover_rate;
            end
        else
            fprintf('警告：日期 %s 的权重数据为空，跳过该日期\n', current_date);
        end
        
        if mod(i, 50) == 0 || i == length(date_list)
            fprintf('已处理 %d/%d 个交易日\n', i, length(date_list));
        end
    end
end

function [portfolio_net_value, portfolio_dates] = calculate_net_value(portfolio_returns, turnover_rates, date_list, cost_rate)
    % 计算投资组合净值
    
   
    portfolio_net_value = zeros(length(portfolio_returns), 1);
    
    % 确保数据长度一致，基于实际处理的数据长度
    data_length = length(portfolio_returns);
    
    % 转换日期格式，只取实际处理的数据长度
    if iscell(date_list)
        if length(date_list) >= data_length
            portfolio_dates = datetime(date_list(1:data_length), 'InputFormat', 'yyyy-MM-dd');
        else
            % 如果date_list长度不足，创建默认日期序列
            portfolio_dates = datetime(1:data_length, 'ConvertFrom', 'datenum');
        end
    else
        if length(date_list) >= data_length
            portfolio_dates = date_list(1:data_length);
        else
            % 如果date_list长度不足，创建默认日期序列
            portfolio_dates = datetime(1:data_length, 'ConvertFrom', 'datenum');
        end
    end
    
    % 从收益率中扣除换手成本（匹配Python逻辑）
    portfolio_returns_after_cost = portfolio_returns - (turnover_rates * cost_rate);
    
    portfolio_dates.Format = 'yyyy-MM-dd';



    
    % 使用cumprod计算净值，与Python版本完全一致
    portfolio_net_value = cumprod(1 + portfolio_returns_after_cost);
    
    
    
    
    fprintf('=== 投资组合净值计算完成 ===\n');
    fprintf('起始净值: %g (基准日期: %s)\n', portfolio_net_value(1), string(portfolio_dates(1)));
    fprintf('最终净值: %g (结束日期: %s)\n', portfolio_net_value(end), string(portfolio_dates(end)));
    fprintf('总收益率: %g%%\n', (portfolio_net_value(end) - 1) * 100);
    
    % 返回组合收益率数据（扣除换手成本后）
    portfolio_returns = portfolio_returns_after_cost;
end
