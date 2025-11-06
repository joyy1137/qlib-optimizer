function append_log_to_file(log_file_or_fmt, varargin)
% append_log_to_file - append timestamped message to a log file.
% Usage:
%   append_log_to_file(fmt, args...)
%   append_log_to_file(log_file, fmt, args...)
% If the first argument looks like a path (contains filesep), it is used as
% the target log file; otherwise a default repo-level logs/weight_optimizer.log
% is used and the first arg is treated as the format string.

try
    % Determine whether first arg is a path or format string
    if nargin == 0
        return;
    end

    first = log_file_or_fmt;
    if ischar(first) && contains(first, filesep)
        % first is a path
        log_file = first;
        if nargin >= 2
            fmt = varargin{1};
            args = varargin(2:end);
        else
            return;
        end
    else
        % Use default log file next to repository root
        script_dir = fileparts(mfilename('fullpath'));
        % script_dir -> .../Optimizer_matlab/utils
        repo_root = fileparts(script_dir); % .../Optimizer_matlab
        log_dir = fullfile(repo_root, '..', 'logs');
        if ~exist(log_dir, 'dir')
            try mkdir(log_dir); catch, end
        end
        log_file = fullfile(log_dir, 'weight_optimizer.log');
        fmt = first;
        args = varargin;
    end

    % Format message
    try
        if ~isempty(args)
            msg = sprintf(fmt, args{:});
        else
            msg = fmt;
        end
    catch
        % If formatting fails, fallback to concatenation
        try
            msg = sprintf('%s', fmt);
        catch
            msg = '<log formatting error>';
        end
    end

    % Prepend timestamp and ensure newline
    try
        timestamp = datestr(now, 'yyyy-mm-dd HH:MM:SS');
        if isempty(msg) || msg(end) ~= char(10)
            msg = [msg char(10)];
        end
        out = sprintf('%s %s', timestamp, msg);
    catch
        out = [datestr(now) ' ' msg char(10)];
    end

    % Append to file, ignore errors
    try
        fid = fopen(log_file, 'a');
        if fid > 0
            fwrite(fid, out);
            fclose(fid);
        end
    catch
        % ignore
    end
catch
    % swallow all errors to avoid impacting main flow
end
end
