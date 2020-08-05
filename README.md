# Execution of Jupyter notebooks on a SLURM cluster

You can execute long-running notebooks on the cluster so you do not have to wait for them to complete.

You can also easily run the same notebook with different parameter settings.

## Executing a notebook

Execute notebooks with parameters on slurm

Execute a notebook `notebook1.ipynb` crating a new file called `notebook1.nbconvert.ipynb`:

    python slurm-jupyter-run.py notebook1.ipynb

Execute a notebook `notebook1.ipynb` inplace:

    python slurm-jupyter-run.py --inplace notebook1.ipynb

Execute a notebook `notebook1.ipynb` crating a html file called `notebook1.html`:

    python slurm-jupyter-run.py --format html notebook1.ipynb

## Executing multiple notebooks in sequence

To execute several notebooks in order just add them as arguments in the right order:

    python slurm-jupyter-run.py notebook1.ipynb notebook1.ipynb

## Executing a notebook multiple times with differrent parameters:

You can specify a notebook with cells that are injected at the top of each executed notebook. 

    python slurm-jupyter-run.py --parameters parameters.ipynb notebook1.ipynb
    
    python slurm-jupyter-run.py --parameters parameters.ipynb notebook1.ipynb notebook2.ipynb 

    python slurm-jupyter-run.py --parameters parameters.ipynb --format html notebook1.ipynb

See `parameters.ipynb` for an example.
