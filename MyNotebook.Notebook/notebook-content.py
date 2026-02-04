# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "00000000-0000-0000-0000-000000000000",
# META       "default_lakehouse_name": "LH_Sample",
# META       "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000000",
# META       "known_lakehouses": [
# META         {
# META           "id": "00000000-0000-0000-0000-000000000000"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "00000000-0000-0000-0000-000000000000",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Code Integration in PySpark Notebook

# MARKDOWN ********************

# ## Prerequisites

# MARKDOWN ********************

# Items to be created: Lakehouse, environment, notebook with name "NB_Utils" with defined function test_function()

# MARKDOWN ********************

# ## Imports

# CELL ********************

import json
import sempy.fabric as fabric

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Parameters

# CELL ********************

WS_NAME = ""
WS_ID = ""
LH_NAME = ""
LH_ID = ""
LH_DIR = ""
MODULE_NAME = ""
ENV_NAME = ""
ENV_ID = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Magic Commands

# CELL ********************

%run NB_Utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test_function()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Pros: 
# - fast and easy
# - flexibility
# - accessibility 
# - git integrated
# 
# Cons: 
# - Cause messy notebook snapshot
# - Limited size, can break scheduled runs when too
# - limited to the same workspace
# - not supported in Python NBs

# MARKDOWN ********************

# ## 2. Environments

# MARKDOWN ********************

# Pros: 
# - git integrated
# - Azure artifact feeds
# - cross-workspace
# - REST API endpoint
# 
# Cons: 
# - higher complexity
# - additional publish step
# - slow start up times

# CELL ********************

def get_notebook_code(nb_name, ws_id):
    nb = notebookutils.notebook.getDefinition(nb_name, ws_id)
    cells = json.loads(nb)["cells"]
    code_cells = [item for item in cells if item['cell_type'] == 'code']
    code = '\n'.join(''.join(item['source']) for item in code_cells if 'source' in item)
    return code

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def refresh_environment_module_from_notebook(nb_name, ws_id, env_id, module_name):
    code = get_notebook_code(nb_name, ws_id)

    client = fabric.FabricRestClient()
    # load python code to environment 
    client.post(f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/environments/{env_id}/staging/libraries", files={'file': (module_name, code)})
    # publish environment
    response = client.post(f'https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/environments/{env_id}/staging/publish')
    return response

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

refresh_environment_module_from_notebook("NB_Utils", WS_ID, ENV_ID, MODULE_NAME)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from utils import test_function

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test_function()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Notebook Builtin Resources

# CELL ********************

def refresh_builtin_resources_from_lakehouse_file(lh_name, ws_name, lh_dir, module_name):
    if notebookutils.fs.exists('file:/synfs/nb_resource/builtin/modules'):
        notebookutils.fs.rm('file:/synfs/nb_resource/builtin/modules',True)

    abfs_path = f"abfss://{ws_name}@onelake.dfs.fabric.microsoft.com/{lh_name}.Lakehouse/{lh_dir}/{module_name}"
    file_path = notebookutils.fs.ls(abfs_path)[0].path
    new_file_name = notebookutils.fs.ls(abfs_path)[0].name

    notebookutils.fs.cp(f'{file_path}',
        f'file:/synfs/nb_resource/builtin/modules/{new_file_name}', True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

refresh_builtin_resources_from_lakehouse_file(
    ws_name = WS_NAME,
    lh_name = LH_NAME,
    lh_dir = LH_DIR,
    module_name = MODULE_NAME
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import builtin.modules.utils as U

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

U.test_function()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def load_notebook_to_builtin_resource(nb_name, ws_id, module_name):
    code = get_notebook_code(nb_name, ws_id)
    base_path = "/synfs/nb_resource/builtin/modules"
    full_path = f"{base_path}/{module_name}"

    if not os.path.exists(base_path):
        os.makedirs(base_path)

    with open(full_path, "w", encoding="utf-8") as f:
        f.write(code)
    
    print(f"Successfully loaded {nb_name} into {full_path}")
    return full_path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

load_notebook_to_builtin_resource(
    nb_name = "NB_Utils",
    ws_id = WS_ID,
    module_name= MODULE_NAME
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from builtin.modules.utils import test_function

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test_function()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. File section

# CELL ********************

def refresh_lakehouse_file_from_notebook(nb_name, ws_id, ws_name, lh_name, lh_dir, file_name):
    code = get_notebook_code(nb_name, ws_id)
    abfs_path = f"abfss://{ws_name}@onelake.dfs.fabric.microsoft.com/{lh_name}.Lakehouse/{lh_dir}/{file_name}"

    notebookutils.fs.put(abfs_path, code, True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

refresh_lakehouse_file_from_notebook(
        nb_name = "NB_Utils", 
        ws_id = WS_ID,
        ws_name = WS_NAME, 
        lh_name = LH_NAME, 
        lh_dir = LH_DIR, 
        file_name = MODULE_NAME
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

module_path = f"abfss://{WS_NAME}@onelake.dfs.fabric.microsoft.com/{LH_NAME}.Lakehouse/{LH_DIR}/{MODULE_NAME}"
sc.addPyFile(module_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from utils import test_function

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test_function()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Pros: 
# - fast and easy
# - accessibility 
# - available in Python notebooks
# 
# Cons: 
# - not git integrated
# - need automation
