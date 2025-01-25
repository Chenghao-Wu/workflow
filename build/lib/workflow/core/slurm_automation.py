import subprocess
import logging
import time
from typing import List, Dict

class SlurmJobManager:
    def __init__(self, max_concurrent_jobs: int = 50):
        self.max_concurrent_jobs = max_concurrent_jobs
        self.jobs: List[Dict] = []
        self.running_jobs: Dict[str, str] = {}  # job_name -> slurm_id
        self.setup_logging()
    
    def setup_logging(self):
        logging.basicConfig(level=logging.INFO)
    
    def generate_sbatch_command(self, job: Dict) -> str:
        """Generate sbatch command with all parameters"""
        cmd = ["sbatch"]
        
        # Add basic Slurm parameters
        cmd.extend([
            f"--job-name={job['name']}",
            f"--partition={job['partition']}",
            f"--mem={job['memory']}",
            f"--time={job['time_limit']}"
        ])
        
        # Add optional parameters if present
        if 'nodes' in job:
            cmd.append(f"--nodes={job['nodes']}")
        if 'ntasks' in job:
            cmd.append(f"--ntasks={job['ntasks']}")
        if 'cpus_per_task' in job:
            cmd.append(f"--cpus-per-task={job['cpus_per_task']}")
        if 'qos' in job:
            cmd.append(f"--qos={job['qos']}")
            
        # Handle dependencies
        if 'depends_on' in job and job['depends_on']:
            if isinstance(job['depends_on'], list):
                dependency_str = ':'.join(job['depends_on'])
                cmd.append(f"--dependency=afterok:{dependency_str}")
            else:
                cmd.append(f"--dependency=afterok:{job['depends_on']}")
        
        # Add output and error file paths
        cmd.extend([
            f"--output=slurm_logs/{job['name']}_%j.out",
            f"--error=slurm_logs/{job['name']}_%j.err"
        ])
        
        # Add the script path
        cmd.append(job['script_path'])
        
        return " ".join(cmd)
    
    def add_job(self, name: str, script_path: str, working_dir: str = ".", **slurm_params) -> str:
        """Add a new job to the queue and return its Slurm job ID"""
        job = {
            'name': name,
            'script_path': script_path,
            'working_dir': working_dir,
            **slurm_params
        }
        
        try:
            cmd = self.generate_sbatch_command(job)
            process = subprocess.Popen(
                cmd,
                shell=True,
                cwd=working_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            stdout, stderr = process.communicate()
            
            if process.returncode == 0:
                # Extract job ID from sbatch output (format: "Submitted batch job 123456")
                job_id = stdout.strip().split()[-1]
                job['slurm_id'] = job_id
                job['status'] = 'submitted'
                self.jobs.append(job)
                self.running_jobs[name] = job_id
                logging.info(f"Submitted job: {name} (Slurm ID: {job_id})")
                return job_id
            else:
                logging.error(f"Error submitting job {name}: {stderr}")
                raise Exception(f"Job submission failed: {stderr}")
                
        except Exception as e:
            logging.error(f"Error submitting job {name}: {str(e)}")
            raise
    
    def check_job_status(self, slurm_id: str) -> str:
        """Check the status of a Slurm job"""
        cmd = f"sacct -j {slurm_id} --format=State --noheader --parsable2"
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            if result.returncode == 0:
                status = result.stdout.strip().split('\n')[0]
                return status
            return "UNKNOWN"
        except Exception:
            return "UNKNOWN"
    
    def check_running_jobs(self):
        """Check status of running jobs and update accordingly"""
        completed_jobs = []
        
        for job_name, slurm_id in self.running_jobs.items():
            status = self.check_job_status(slurm_id)
            job = next(j for j in self.jobs if j['name'] == job_name)
            
            if status in ["COMPLETED", "FAILED", "CANCELLED", "TIMEOUT"]:
                job['end_time'] = time.time()
                job['status'] = "completed" if status == "COMPLETED" else "failed"
                logging.info(f"Job {job_name} (Slurm ID: {slurm_id}) completed with status: {status}")
                completed_jobs.append(job_name)
        
        # Remove completed jobs from running_jobs
        for job_name in completed_jobs:
            del self.running_jobs[job_name]
    
    def run_jobs(self):
        """Main method to run and manage jobs"""
        while (self.running_jobs or 
               any(job['status'] == "pending" for job in self.jobs)):
            # Check running jobs
            self.check_running_jobs()
            
            # Wait before checking again
            time.sleep(10)  # Check status every 10 seconds
            
        logging.info("All jobs completed")