from pathlib import Path
from typing import Dict, List, Optional, Union
from dataclasses import dataclass
import yaml
try:
    from automate.slurm_automation import SlurmJobManager
except ImportError:
    from workflow.core.slurm_automation import SlurmJobManager  # Changed from relative to absolute import
    
@dataclass
class SimulationParameters:
    # System parameters
    temperature: float
    pressure: float = 1.0  # in atm
    timestep: float = 1.0  # in fs
    
    # Run parameters
    minimization_steps: int = 10000
    equilibration_steps: int = 1000000
    production_steps: int = 5000000
    
    # Output parameters
    thermo_freq: int = 1000
    dump_freq: int = 1000
    restart_freq: int = 10000
    
    # Force field parameters
    pair_style: str = "lj/cut/coul/long"
    pair_cutoff: float = 10.0
    kspace_style: str = "pppm"
    kspace_accuracy: float = 1e-4
    
    # GPU parameters
    use_gpu: bool = False
    gpu_package: str = "gpu"
    gpu_suffix: str = "hybrid"
    
    # Slurm parameters
    partition: str = "gpu"
    nodes: int = 1
    ntasks: int = 1
    cpus_per_task: int = 4
    memory: str = "16G"
    time_limit: str = "48:00:00"

class LammpsWorkflow:
    def __init__(self, base_dir: str = "."):
        self.base_dir = Path(base_dir)
        self.slurm_manager = SlurmJobManager(max_concurrent_jobs=10)
        self.setup_directories()
        
    def setup_directories(self):
        """Create necessary directories for simulation"""
        dirs = [
            "scripts",
            "input_files",
            "minimization/trajectory",
            "minimization/restart",
            "equilibration/trajectory",
            "equilibration/restart",
            "production/trajectory",
            "production/restart",
            "analysis"
        ]
        for dir_path in dirs:
            (self.base_dir / dir_path).mkdir(parents=True, exist_ok=True)

    def create_lammps_script(self, phase: str, params: SimulationParameters) -> str:
        """Create LAMMPS input script for a specific phase"""
        script_path = self.base_dir / "input_files" / f"{phase}.in"
        
        content = self._generate_header(params)
        content += self._generate_system_setup()
        content += self._generate_force_field(params)
        
        if phase == "minimization":
            content += self._generate_minimization(params)
        elif phase == "equilibration":
            content += self._generate_equilibration(params)
        elif phase == "production":
            content += self._generate_production(params)
            
        with open(script_path, "w") as f:
            f.write(content)
            
        return str(script_path)

    def create_submission_script(self, phase: str, params: SimulationParameters) -> str:
        """Create submission script for a specific phase"""
        script_path = self.base_dir / "scripts" / f"submit_{phase}.sh"
        
        content = f"""#!/bin/bash
# Load required modules
module purge
module load lammps

# Set OpenMP threads
export OMP_NUM_THREADS={params.cpus_per_task}

# Run LAMMPS
{"mpirun -np " + str(params.ntasks) if params.ntasks > 1 else ""} lmp -in input_files/{phase}.in \\
    -var temperature {params.temperature} \\
    -var pressure {params.pressure} \\
    -var timestep {params.timestep} \\
    -var is_gpu {1 if params.use_gpu else 0}
"""
        
        with open(script_path, "w") as f:
            f.write(content)
        
        script_path.chmod(0o755)
        return str(script_path)

    def submit_workflow(self, params: SimulationParameters, name: str) -> None:
        """Submit complete workflow to Slurm"""
        phases = ["minimization", "equilibration", "production"]
        previous_job_id = None
        
        for phase in phases:
            # Create LAMMPS input script
            lammps_script = self.create_lammps_script(phase, params)
            
            # Create submission script
            submit_script = self.create_submission_script(phase, params)
            
            # Prepare Slurm parameters
            slurm_params = {
                "partition": params.partition,
                "nodes": params.nodes,
                "ntasks": params.ntasks,
                "cpus_per_task": params.cpus_per_task,
                "memory": params.memory,
                "time_limit": params.time_limit
            }
            
            # Add job dependency if not first phase
            if previous_job_id:
                slurm_params["depends_on"] = previous_job_id
            
            # Submit job
            job_id = self.slurm_manager.add_job(
                name=f"{name}_{phase}",
                script_path=submit_script,
                working_dir=str(self.base_dir),
                **slurm_params
            )
            
            previous_job_id = job_id

    def _generate_header(self, params: SimulationParameters) -> str:
        """Generate LAMMPS header section"""
        header = f"""# LAMMPS input script for all-atom simulation
# Generated by LammpsInputGenerator

# Initialization
units           real
atom_style      full
boundary        p p p

"""
        if params.use_gpu:
            header += f"""# GPU configuration
package         gpu {params.gpu_package}
suffix          {params.gpu_suffix}

"""
        return header
    
    def _generate_system_setup(self) -> str:
        """Generate system setup section"""
        return """# Read system data
read_data       system.data
include         "system.in.settings"
include         "system.in.charges"

"""
    
    def _generate_force_field(self, params: SimulationParameters) -> str:
        """Generate force field section"""
        return f"""# Force field setup
pair_style      {params.pair_style} {params.pair_cutoff}
bond_style      hybrid harmonic
angle_style     hybrid harmonic
dihedral_style  hybrid opls
improper_style  hybrid harmonic

# Long-range solver
kspace_style    {params.kspace_style} {params.kspace_accuracy}

# Mixing rules
pair_modify     mix geometric tail yes
special_bonds   lj/coul 0.0 0.0 0.5

"""
    
    def _generate_minimization(self, params: SimulationParameters) -> str:
        """Generate minimization section"""
        return f"""# Energy minimization
thermo          {params.thermo_freq}
thermo_style    custom step temp pe ke etotal press vol density

minimize        1.0e-4 1.0e-6 {params.minimization_steps} {params.minimization_steps*10}

write_restart   minimization/min.restart

"""
    
    def _generate_equilibration(self, params: SimulationParameters) -> str:
        """Generate equilibration section"""
        return f"""# Equilibration
reset_timestep  0

# Temperature and pressure control
fix             1 all momentum 1000 linear 1 1 1 angular
fix             2 all npt temp {params.temperature} {params.temperature} 100.0 iso {params.pressure} {params.pressure} 1000.0

# Output settings
thermo          {params.thermo_freq}
thermo_style    custom step temp press pe ke etotal ebond eangle epair lx ly lz vol density

dump            1 all custom {params.dump_freq} equilibration/trajectory/equ.*.lammpstrj id type x y z vx vy vz
dump_modify     1 sort id

restart         {params.restart_freq} equilibration/restart/equ.*.restart

# Run equilibration
timestep        {params.timestep}
run            {params.equilibration_steps}

write_restart  equilibration/equ.final.restart

"""
    
    def _generate_production(self, params: SimulationParameters) -> str:
        """Generate production section"""
        return f"""# Production
reset_timestep  0

# Temperature and pressure control
fix             1 all momentum 1000 linear 1 1 1 angular
fix             2 all npt temp {params.temperature} {params.temperature} 100.0 iso {params.pressure} {params.pressure} 1000.0

# Output settings
thermo          {params.thermo_freq}
thermo_style    custom step temp press pe ke etotal ebond eangle epair lx ly lz vol density

dump            1 all custom {params.dump_freq} production/trajectory/prod.*.lammpstrj id type x y z vx vy vz
dump_modify     1 sort id

restart         {params.restart_freq} production/restart/prod.*.restart

# Run production
timestep        {params.timestep}
run            {params.production_steps}

write_restart  production/prod.final.restart

"""

    def run_all(self):
        """Run all submitted jobs"""
        self.slurm_manager.run_jobs()

# Example usage
if __name__ == "__main__":
    # Initialize workflow
    workflow = LammpsWorkflow(base_dir="polymer_simulation")
    
    # Define simulation parameters
    systems = [
        {
            "name": "polymer_300K",
            "params": SimulationParameters(
                temperature=300.0,
                pressure=1.0,
                use_gpu=True,
                timestep=2.0,
                minimization_steps=50000,
                equilibration_steps=2000000,
                production_steps=5000000,
                partition="gpu",
                ntasks=4,
                memory="32G"
            )
        },
        {
            "name": "polymer_350K",
            "params": SimulationParameters(
                temperature=350.0,
                pressure=1.0,
                use_gpu=True,
                timestep=2.0,
                minimization_steps=50000,
                equilibration_steps=2000000,
                production_steps=5000000,
                partition="gpu",
                ntasks=4,
                memory="32G"
            )
        }
    ]
    
    # Submit all simulations
    for system in systems:
        workflow.submit_workflow(system["params"], system["name"])
    
    # Run all jobs
    workflow.run_all() 