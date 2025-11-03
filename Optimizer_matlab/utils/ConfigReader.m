function [portfolio_info, portfolio_constraint, factor_constraint] = ConfigReader(config_path)
% ConfigReader - 配置读取模块
% 读取Excel配置文件并返回相关数据
%
% 输入:
%   config_path - 配置文件路径
%
% 输出:
%   portfolio_info - 投资组合信息表
%   portfolio_constraint - 投资组合约束表
%   factor_constraint - 因子约束表

    % 读取 portfolio_info sheet
    portfolio_info = readtable(config_path, 'Sheet', 'portfolio_info');

    % 读取 portfolio_constraint sheet
    opts = detectImportOptions(config_path, 'Sheet', 'portfolio_constraint');
    opts = setvartype(opts, opts.VariableNames, 'char');
    portfolio_constraint = readtable(config_path, opts);

    % 读取 factor_constraint sheet
    factor_constraint = readtable(config_path, 'Sheet', 'factor_constraint');

    fprintf('成功读取配置文件:\n');
    fprintf('  投资组合数量: %d\n', height(portfolio_info));
    
    % 显示投资组合信息
    if height(portfolio_info) > 0
        fprintf('  投资组合列表:\n');
        for i = 1:height(portfolio_info)
            fprintf('    %d. %s\n', i, portfolio_info.portfolio_name{i});
        end
    end
end
