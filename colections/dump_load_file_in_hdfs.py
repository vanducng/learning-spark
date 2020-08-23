import joblib
import subprocess

def run_cmd(args_list):
    """
    run linux commands
    """
    # import subprocess
    print('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return =  proc.returncode

    return s_return, s_output, s_err 

# Dump file to disk (within application container)
model_variable = {
    "key": 123,

}

model_name = "model"
joblib.dump(model_variable, model_name)

# Load file
model_variable = joblib.load(model_name)

# Put model to hdfs
model_hdfs_path = f"hdfs://nameservice1/user/duc.nguyenv3/projects/01_lgbm_modeling/data/output"
run_cmd(["hdfs", "dfs", "-put", "-f", model_name, model_hdfs_path])

# Get and overwrite to local file
run_cmd(["rm", "-rf", model_name])
run_cmd(["hdfs", "dfs", "-get", f"{model_hdfs_path}/{model_name}", "."])

