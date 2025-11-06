function run_optimizer()
	javaaddpath("E:\qlib-optimizer\Optimizer_matlab\mysql-connector-j-9.3.0.jar")
	addpath(genpath('E:\YAMLMatlab_0.4.3'));
	savepath;
	data_preparation();
	batch_run_optimizer();


	currentFile = mfilename('fullpath');
	currentDir = fileparts(currentFile);

	path_config = fullfile(currentDir, '..','config', 'paths.yaml');

	path = ReadYaml(path_config);

	addpath(fullfile(currentDir, 'utils'));
	addpath(fullfile(currentDir, 'tools'));

	merge_portfolio_dataframe(path.temp_dir);


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
