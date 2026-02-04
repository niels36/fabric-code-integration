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

# # Code Integration in Python Notebook

# MARKDOWN ********************

# ## Prerequisites

# MARKDOWN ********************

# Items to be created: Lakehouse, environment, notebook with name "NB_Utils" with defined function test_function()

# MARKDOWN ********************

# ## Imports

# CELL ********************

import os
import sys
import json
import sempy.fabric as fabric

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
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
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## 1. Magic Commands

# MARKDOWN ********************

# not supported :(

# MARKDOWN ********************

# ## 2. Environments

# MARKDOWN ********************

# not supported :(

# MARKDOWN ********************

# ## 3. Notebook Builtin Resources

# CELL ********************

def refresh_lakehouse_file_from_notebook(nb_name, ws_id, ws_name, lh_name, lh_dir, file_name):
    code = get_notebook_code(nb_name, ws_id)
    abfs_path = f"abfss://{ws_name}@onelake.dfs.fabric.microsoft.com/{lh_name}.Lakehouse/{lh_dir}/{file_name}"

    notebookutils.fs.put(abfs_path, code, True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

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
# META   "language_group": "jupyter_python"
# META }

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
# META   "language_group": "jupyter_python"
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
# META   "language_group": "jupyter_python"
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
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import builtin.modules.utils as U

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

U.test_function()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
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
# META   "language_group": "jupyter_python"
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
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

from builtin.modules.utils import test_function

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

test_function()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## 4. File section

# MARKDOWN ********************

# ### Option 1: From Default Lakehouse

# CELL ********************

sys.path.append(f'/lakehouse/default/{LH_DIR}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

from utils import test_function

test_function()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Option 2: From abfs path

# CELL ********************

import fsspec

filesystem_code = "abfss"
storage_options = {
        "account_name": "onelake",
        "account_host": "onelake.dfs.fabric.microsoft.com"
    }
onelake_fs = fsspec.filesystem(filesystem_code, **storage_options)

utils_path = f"abfss://{WS_ID}@onelake.dfs.fabric.microsoft.com/{LH_ID}/{LH_DIR}/{MODULE_NAME}"
utils_namespace = {}

with onelake_fs.open(utils_path, 'r') as f:
    exec(f.read(), utils_namespace)

# Import specific functions into globals
function_list = ['test_function']
for name in function_list:
    if name in utils_namespace:
        globals()[name] = utils_namespace[name]

test_function()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
