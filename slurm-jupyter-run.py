#!/usr/bin/env python
from __future__ import (absolute_import, division, print_function, unicode_literals)
import subprocess
import sys
import os
import re
import platform
import getpass
import time
import argparse
from textwrap import wrap
from subprocess import PIPE, Popen

import json, copy

def modpath(p, parent=None, base=None, suffix=None):
    par, name = os.path.split(p)
    name_no_suffix, suf = os.path.splitext(name)
    if type(suffix) is str:
        suf = suffix
    if parent is not None:
        par = parent
    if base is not None:
        name_no_suffix = base

    new_path = os.path.join(par, name_no_suffix + suf)
    if type(suffix) is tuple:
        assert len(suffix) == 2
        new_path, nsubs = re.subn(r'{}$'.format(suffix[0]), suffix[1], new_path)
        assert nsubs == 1, nsubs
    return new_path

def str_to_mb(s):
    # compute mem in mb
    scale = s[-1].lower()
    assert scale in ['k', 'm', 'g']
    memory_per_cpu_mb = float(s[:-1])
    if scale == 'g':
        memory_per_cpu_mb *= 1024
    if scale == 'k':
        memory_per_cpu_mb /= 1024.0
    return memory_per_cpu_mb

def execute(cmd, stdin=None):
    process = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE, shell=True)
    stdout, stderr = process.communicate(stdin)
    return stdout, stderr

def submit_slurm_job(spec):

    script = slurm_script.format(**spec)
    if args.verbose: print("slurm script:", script, sep='\n')

    # if sys.version_info >= (3,0):
    #     script = script.encode()

    tmp_script_path = "{tmp_dir}/{tmp_script}".format(**spec)
    with open(tmp_script_path, 'w') as f:
        f.write(script)

    cmd = '{slurm} && sbatch {tmp_dir}/{tmp_script} '.format(**spec)
    if args.verbose: print("command:", cmd, sep='\n')
   
    stdout, stderr = execute(cmd) # hangs untill submission

    # get stdour and stderr and get jobid
    if sys.version_info >= (3,0):
        stdout = stdout.decode()
        stderr = stderr.decode()
    try:
        job_id = re.search('Submitted batch job (\d+)', stdout).group(1)
    except AttributeError:
        print('Slurm job submission failed')
        print(stdout)
        print(stderr)
        sys.exit()
    print("Submitted slurm with job id:", job_id)

    return job_id

def parse_parameter_notebook(notebook_json):

    spike_in_cells = list()
    suffixes = list()
    for cell in notebook_json['cells']:
        if cell['cell_type'] == 'code':
            spike_in_cell = {'cell_type': 'code', 'execution_count': 0, 'metadata': {}, 
                'outputs': [], 'source': cell['source']}
            spike_in_cells.append(spike_in_cell)
        if cell['cell_type'] == 'raw':
            name = cell['source'][0].split()
            assert len(name) == 1
            suffixes.append(name[0])

    if not suffixes or not len(spike_in_cells) == len(suffixes):
        suffixes = list(map(str, range(len(spike_in_cells))))

    return spike_in_cells, suffixes


# string template for slurm script
slurm_script =  """#!/bin/sh
#SBATCH -p {queue}
{memory_spec}
#SBATCH -n {nr_nodes}
#SBATCH -c {nr_cores}
#SBATCH -t {walltime}
#SBATCH -o {tmp_dir}/{tmp_name}.%j.out
#SBATCH -e {tmp_dir}/{tmp_name}.%j.err
#SBATCH -J {job_name}
{account_spec}
{sources_loaded}
##cd "{cwd}"

if [ -d "$HOME/anaconda3" ]
then
    # >>> conda initialize >>>
    # !! Contents within this block are managed by 'conda init' !!
    __conda_setup="$('$HOME/anaconda3/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
    if [ $? -eq 0 ]; then
        eval "$__conda_setup"
    else
        if [ -f "$HOME/anaconda3/etc/profile.d/conda.sh" ]; then
            . "$HOME/anaconda3/etc/profile.d/conda.sh"
        else
            export PATH="$HOME/anaconda3/bin:$PATH"
        fi
    fi
    unset __conda_setup
    # <<< conda initialize <<<
else
    # >>> conda initialize >>>
    # !! Contents within this block are managed by 'conda init' !!
    __conda_setup="$('$HOME/miniconda3/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
    if [ $? -eq 0 ]; then
        eval "$__conda_setup"
    else
        if [ -f "$HOME/miniconda3/etc/profile.d/conda.sh" ]; then
            . "$HOME/miniconda3/etc/profile.d/conda.sh"
        else
            export PATH="$HOME/miniconda3/bin:$PATH"
        fi
    fi
    unset __conda_setup
    # <<< conda initialize <<<
fi

{environment}
{ipcluster}
unset XDG_RUNTIME_DIR

{commands}
"""


description = """
The script executes a notebook on the cluster"""

not_wrapped = """See github.com/kaspermunch/slurm_jupyter_run for documentation and common use cases."""

description = "\n".join(wrap(description.strip(), 80)) + "\n\n" + not_wrapped

parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=description)

# slurm arguments
parser.add_argument("-A", "--account",
                  dest="account",
                  type=str,
                  default=None,
                  help="Account/Project to run under. This is typically the name of the shared folder you work in. Not specifying an account decreases your priority in the cluster queue.")
parser.add_argument("-q", "--queue",
                  dest="queue",
                  type=str,
                  choices=['normal', 'express', 'fat1', 'fat2', 'gpu'],
                  default="normal",
                  help="Cluster queue to submit to.")
parser.add_argument("-c", "--cores",
                  dest="cores",
                  type=int,
                  default=1,
                  help="Number of cores. For multiprocessing or for running more than one notebook simultaneously.")
parser.add_argument("-n", "--nodes",
                  dest="nodes",
                  type=int,
                  default=1,
                  help="Number of nodes (machines) to allocate.")       
parser.add_argument("--ipcluster",
                  dest="ipcluster",
                  action='store_true',
                  default=False,
                  help="Start an ipcluster")
parser.add_argument("-t", "--time",
                  dest="time",
                  type=str,
                  default="08:00:00",
                  help="Max wall time. specify as HH:MM:SS (or any other format supported by the cluster). The jupyter server is killed automatically after this time.")
parser.add_argument("-N", "--name",
                  dest="name",
                  type=str,
                  default="jptr_{}_{}".format(getpass.getuser(), int(time.time())),
                  help="Name of job. Only needed if you run multiple servers and want to be able to recognize a particular one in the cluster queue.")
parser.add_argument("-e", "--environment",
                  dest="environment",
                  type=str,
                  default='',
                  help="Conda environment to run jupyter in.")
parser.add_argument("-v", "--verbose",
                  dest="verbose",
                  action='store_true',
                  help="Print debugging information")

group = parser.add_mutually_exclusive_group(required=False)
group.add_argument("--memory-per-cpu",
                  dest="memory_per_cpu",
                  type=str,
                  help="Max memory for each core in gigabytes or megabytes e.g. 4g or 50m")
group.add_argument("-m", "--total-memory",
                  dest="total_memory",
                  type=str,
                  default='8g',
                  help="Max memory total for all cores in gigabytes or megabytes . e.g. 4g or 50m")

# nbconvert arguments
parser.add_argument("--timeout",
                  dest="timeout",
                  type=int,
                  default='-1',
                  help="Cell execution timeout in seconds. Default -1. No timeout.")
parser.add_argument("--allow-errors",
                  dest="allow_errors",
                  action='store_true',
                  help="Allow errors in cell executions.")
parser.add_argument("--format",
                  dest="format",
                  choices=['notebook', 'html', 'pdf'],
                  default='notebook',
                  help="Output format.") 
parser.add_argument("--inplace",
                  dest="inplace",
                  action='store_true',
                  help="Output format.")
parser.add_argument("--cleanup",
                  dest="cleanup",
                  action='store_true',
                  help="Removes un-executed notebooks generated using --parameters and format other than 'notebook'")                           

parser.add_argument("-p", "--parameters",
                  dest="parameters",
                  help="Python file with parameters that is executed ")       

parser.add_argument('notebooks', nargs='*')

args = parser.parse_args()


if args.nodes != 1:
    print("Multiprocessign across multiple nodes not supported yet - sorry")
    sys.exit()

if args.inplace and args.format != 'notebook':
    print('Only not use --inplace with other formats than "notebook" format')
    sys.exit()

if args.cleanup and not args.parameters:
    print("Only use --cleanup with --parameters")
    sys.exit()

home = os.path.expanduser("~")

spec = {'environment': args.environment,
        'walltime': args.time,
        'account': args.account,
        'queue': args.queue,
        'nr_cores': args.cores,
        'nr_nodes': args.nodes,
        'cwd': os.getcwd(),
        'sources_loaded': '',
        'slurm': 'source /com/extra/slurm/14.03.0/load.sh',
        'tmp_name': 'slurm_jupyter_run',
        'tmp_dir': home+'/.slurm_jupyter_run',
        'tmp_script': 'slurm_jupyter_run_{}.sh'.format(int(time.time())),
        'job_name': args.name,
        'job_id': None,
        'timeout': args.timeout,
        'format': args.format,
        'inplace': args.inplace,
        }

if not os.path.exists(spec['tmp_dir']):
    os.makedirs(spec['tmp_dir'])

tup = spec['walltime'].split('-')
if len(tup) == 1:
    days, (hours, mins, secs) = 0, tup[0].split(':')
else:
    days, (hours, mins, secs) = tup[0], tup[1].split(':')
end_time = time.time() + int(days) * 86400 + int(hours) * 3600 + int(mins) * 60 + int(secs)

if args.total_memory:
    spec['memory_spec'] = '#SBATCH --mem {}'.format(int(str_to_mb(args.total_memory)))
else:
    spec['memory_spec'] = '#SBATCH --mem-per-cpu {}'.format(int(str_to_mb(args.memory_per_cpu)))

if args.environment:
    spec['environment'] = "\nsource activate " + args.environment

if args.account:
    spec['account_spec'] = "#SBATCH -A {}".format(args.account)
else:
    spec['account_spec'] = ""

if args.ipcluster:
    spec['ipcluster'] = "ipcluster start -n {} &".format(args.cores)
else:   
    spec['ipcluster'] = ''


if args.allow_errors:
    spec['allow_errors'] = '--allow-errors'
else:
    spec['allow_errors'] = ''

if args.inplace or args.parameters and args.format == 'notebook':
    spec['inplace'] = '--inplace'
else:
    spec['inplace'] = ''



nbconvert_cmd = "jupyter nbconvert --ClearOutputPreprocessor.enabled=True --ExecutePreprocessor.timeout={timeout} {allow_errors} {inplace} --to {format} --execute {notebook}"

notebook_list = args.notebooks

if args.parameters:

    with open(args.parameters) as f:
        notebook_json = json.loads(f.read())

    spike_in_cells, suffixes = parse_parameter_notebook(notebook_json)

    for spike_in_cell, suffix in zip(spike_in_cells, suffixes):

        new_notebook_list = list()

        for notebook_path in notebook_list:

            with open(notebook_path) as f:
                notebook_json = json.loads(f.read())

            notebook_base_name = modpath(notebook_path, parent='', suffix='')
            out_dir = modpath(notebook_path, suffix='')
            os.makedirs(out_dir, exist_ok=True)

            new_json = copy.deepcopy(notebook_json)

            # for i in range(len(new_json['cells'])):
            #     new_json['cells'][i]['outputs'] = []

            new_json['cells'].insert(0, spike_in_cell)
            new_notebook_path = modpath(notebook_path, base=notebook_base_name + '_' + suffix, parent=out_dir)
            with open(new_notebook_path, 'w') as f:
                f.write(json.dumps(new_json))

            new_notebook_list.append(new_notebook_path)

        if args.format != 'notebook' and args.cleanup:
            param_nbconvert_cmd = nbconvert_cmd + ' && rm -f {notebook}'
        else:
            param_nbconvert_cmd = nbconvert_cmd

        command_list = [param_nbconvert_cmd.format(notebook=notebook, **spec) for notebook in new_notebook_list]
        spec['commands'] = ' && '.join(command_list)
        submit_slurm_job(spec)

else:
    command_list = [nbconvert_cmd.format(notebook=notebook, **spec) for notebook in notebook_list]
    spec['commands'] = ' && '.join(command_list)
    submit_slurm_job(spec)



