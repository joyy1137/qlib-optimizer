currentFile = mfilename('fullpath');
currentDir = fileparts(currentFile);
% try
% 	pyScript = fullfile(currentDir, 'qlib_code', 'run_daily_update.py');
% 	pyScript2 = fullfile(currentDir, 'qlib_code', 'export_test_scores_per_day.py');
% 	pythonExe = 'C:\Users\TestUser\.conda\envs\qlib_env\python.exe'; 
% 	cmd = sprintf('"%s" -u "%s"', pythonExe, pyScript);
% 	[status, cmdout] = system(cmd, '-echo');
%     if status ~= 0
%         warning('Running python script failed (status=%d). Output:\n%s', status, cmdout);
%     end
% 	cmd2 = sprintf('"%s" -u "%s"', pythonExe, pyScript2);
% 	[status2, cmdout2] = system(cmd2, '-echo');
%     if status2 ~= 0
%         warning('Running python script failed (status=%d). Output:\n%s', status2, cmdout2);
%     end

% catch ME
% 	% Use identifier-aware warning format to satisfy MATLAB diagnostics
% 	if isprop(ME, 'identifier') && ~isempty(ME.identifier)
% 		id = ME.identifier;
% 	else
% 		id = 'run_optimizer:pythonImportFail';
% 	end
% 	warning(id, 'Failed to launch Python importer: %s', ME.message);
% end
run_optimizer("history");
