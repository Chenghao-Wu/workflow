import sys
sys.path.append('..')
from workflow.core.job_automation import JobManager
from workflow.lammps.lammps import LammpsSimulation, LammpsMDParameters

def main():
    # Initialize managers
    job_manager = JobManager(max_concurrent_jobs=2)
    sim = LammpsSimulation(base_dir="polymer_simulation")
    
    # Define parameters
    params = LammpsMDParameters(
        ensemble="npt",
        thermostat="Nose-Hoover",
        t_start=300.0,
        t_stop=300.0,
        p_start=1.0,
        p_stop=1.0,
        timestep=1.0,
        run_steps=1000000
    )
    
    # Generate input files
    input_files = sim.create_workflow(params)
    
    # Add jobs with dependencies
    min_job = job_manager.add_job(
        name="minimization",
        command=f"lmp -in {input_files[0]}",
        working_dir=str(sim.base_dir)
    )
    
    equ_job = job_manager.add_job(
        name="equilibration",
        command=f"lmp -in {input_files[1]}",
        working_dir=str(sim.base_dir),
        depends_on=min_job
    )
    
    prod_job = job_manager.add_job(
        name="production",
        command=f"lmp -in {input_files[2]}",
        working_dir=str(sim.base_dir),
        depends_on=equ_job
    )
    
    # Run workflow
    job_manager.run_jobs()

if __name__ == "__main__":
    main() 