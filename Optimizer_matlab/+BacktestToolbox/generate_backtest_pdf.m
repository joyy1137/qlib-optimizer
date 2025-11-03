function generate_backtest_pdf(pdf_path, portfolio_name, index_type, start_date, end_date, result_table, performance_metrics, output_dir)
    % 生成回测分析PDF报告
    % 输入:
    %   pdf_path - PDF文件路径
    %   portfolio_name - 投资组合名称
    %   index_type - 指数类型
    %   start_date - 开始日期
    %   end_date - 结束日期
    %   result_table - 回测结果表格
    %   performance_metrics - 业绩指标
    %   output_dir - 输出目录
    
    try
        % 创建PDF文档
        import mlreportgen.dom.*
        import mlreportgen.report.*
        
        % 创建报告对象
        rpt = Report(pdf_path, 'pdf');
        
        % 添加标题
        h = Heading1(sprintf('%s指增分析', index_type));
        h.Style = {HAlign('center'), OuterMargin('2pt','0pt','0pt','0pt'),FontSize('25pt'), Bold(true)};
        add(rpt, h);
        p1 = Paragraph(sprintf('组合名称: %s', portfolio_name));
        p1.Style = {OuterMargin('2pt','0pt','8pt','0pt')}; 
        add(rpt, p1);
        p2 = Paragraph(sprintf('报告生成日期: %s', datestr(now, 'yyyy-mm-dd')));
        p2.Style = {OuterMargin('2pt','0pt','6pt','0pt')};
        add(rpt, p2);
        
        % 一、策略表现
        h = Heading1('一、策略表现');
        h.Style = {OuterMargin('2pt','0pt','12pt','12pt')};
        add(rpt, h);
        
        % 添加业绩指标表格（6列格式）
        if exist('performance_metrics', 'var') && ~isempty(performance_metrics)
            % 从performance_metrics表格中提取数据
            metrics_data = {'年份', '年化收益', '夏普比率', '信息比率', '最大回撤', '年化标准差'};
            
            % 查找各个指标的值
            annual_return_idx = strcmp(performance_metrics.Metric, 'Annual_Return_Pct');
            sharpe_idx = strcmp(performance_metrics.Metric, 'Sharpe_Ratio');
            info_ratio_idx = strcmp(performance_metrics.Metric, 'Info_Ratio');
            max_dd_idx = strcmp(performance_metrics.Metric, 'Max_Drawdown_Pct');
            vol_idx = strcmp(performance_metrics.Metric, 'Annual_Vol_Pct');
            
            % 获取回测期间的年份
            start_year = year(start_date);
            end_year = year(end_date);
            
            % 为每年创建一行数据
            for year_val = start_year:end_year
                data_row = {'', '', '', '', '', ''};
                
                % 年份
                data_row{1} = sprintf('%d', year_val);
                
                % 年化收益
                if any(annual_return_idx)
                    data_row{2} = sprintf('%.2f%%', performance_metrics.Value(annual_return_idx));
                else
                    data_row{2} = 'N/A';
                end
                
                % 夏普比率
                if any(sharpe_idx)
                    data_row{3} = sprintf('%.3f', performance_metrics.Value(sharpe_idx));
                else
                    data_row{3} = 'N/A';
                end
                
                % 信息比率
                if any(info_ratio_idx)
                    data_row{4} = sprintf('%.3f', performance_metrics.Value(info_ratio_idx));
                else
                    data_row{4} = 'N/A';
                end
                
                % 最大回撤
                if any(max_dd_idx)
                    % 最大回撤存储为小数形式，需要乘以100转换为百分比
                    data_row{5} = sprintf('%.2f%%', performance_metrics.Value(max_dd_idx) * 100);
                else
                    data_row{5} = 'N/A';
                end
                
                % 年化标准差
                if any(vol_idx)
                    data_row{6} = sprintf('%.2f%%', performance_metrics.Value(vol_idx));
                else
                    data_row{6} = 'N/A';
                end
                
                % 添加数据行到表格
                metrics_data(end+1,:) = data_row;
            end
            
            % 创建表格并设置边框样式
            metrics_table = FormalTable(metrics_data);
            % 设置表格样式，添加边框
            metrics_table.Style = {Border('solid'), Width('100%')};
            metrics_table.TableEntriesStyle = {Border('solid'), HAlign('center'),OuterMargin('0pt','0pt','4pt','4pt')};
            add(rpt, metrics_table);
        end
        
        % 添加净值走势图
        h = Heading2('净值走势图');
        h.Style = {OuterMargin('2pt','0pt','12pt','8pt')};
        add(rpt, h);

        
        % 检查图片文件是否存在
        benchmark_plot_path = fullfile(output_dir, sprintf('%s_组合基准对比图.png', portfolio_name));
        if exist(benchmark_plot_path, 'file')
            benchmark_img = Image(benchmark_plot_path);
            benchmark_img.Style = {ScaleToFit};
            add(rpt, benchmark_img);
        end
        
        % 添加超额净值图
        add(rpt, Heading2('超额净值走势图'));
        excess_plot_path = fullfile(output_dir, sprintf('%s_超额净值图.png', portfolio_name));
        if exist(excess_plot_path, 'file')
            excess_img = Image(excess_plot_path);
            excess_img.Style = {ScaleToFit};
            add(rpt, excess_img);
        end
        
        % 添加贡献分析图
        add(rpt, Heading2('贡献分析对比图'));
        contribution_plot_path = fullfile(output_dir, sprintf('%s_贡献分析对比图.png', portfolio_name));
        if exist(contribution_plot_path, 'file')
            contribution_img = Image(contribution_plot_path);
            contribution_img.Style = {ScaleToFit};
            add(rpt, contribution_img);
        end
        
        % 生成PDF
        close(rpt);
        
    catch ME
        fprintf('生成PDF报告时出错: %s', ME.message);
        
    end
end
