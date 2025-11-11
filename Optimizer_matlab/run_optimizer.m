function run_optimizer(option)
	javaaddpath("E:\qlib-optimizer\Optimizer_matlab\mysql-connector-j-9.3.0.jar")

	currentFile = mfilename('fullpath');
	currentDir = fileparts(currentFile);

	logDir = fullfile(currentDir, '..', 'logs');
	if ~exist(logDir, 'dir')
		mkdir(logDir);
	end
	logFile = fullfile(logDir, sprintf('weight_optimizer.log'));
	try
		% diary(file) turns on logging to that file
		diary(logFile);
		fprintf_log('Run optimizer logging to: %s\n', logFile);
	catch ME
		warning('Could not start diary log to %s: %s', logFile, ME.message);
	end
	% Ensure diary is turned off on function exit
	cleanupDiary = onCleanup(@() diary('off'));

	
	addpath(genpath('E:\YAMLMatlab_0.4.3'));
	savepath;
	data_preparation(option);
	batch_run_optimizer(option);

	path_config = fullfile(currentDir, '..','config', 'paths.yaml');

	path = ReadYaml(path_config);

	addpath(fullfile(currentDir, 'utils'));
	addpath(fullfile(currentDir, 'tools'));

	merge_portfolio_dataframe(path.temp_dir,option);


	try
		pyScript = fullfile(currentDir, '..', 'qlib_code', 'import_weight_to_mysql.py');
		pythonExe = 'C:\Users\TestUser\.conda\envs\qlib_env\python.exe'; 
		cmd = sprintf('"%s" "%s"', pythonExe, pyScript);
		[status, cmdout] = system(cmd);
		if status ~= 0
			warning('Running python script failed (status=%d). Output:\n%s', status, cmdout);
		
		end
	catch ME
		% Use identifier-aware warning format to satisfy MATLAB diagnostics
		if isprop(ME, 'identifier') && ~isempty(ME.identifier)
			id = ME.identifier;
		else
			id = 'run_optimizer:pythonImportFail';
		end
		warning(id, 'Failed to launch Python importer: %s', ME.message);
	end
end
