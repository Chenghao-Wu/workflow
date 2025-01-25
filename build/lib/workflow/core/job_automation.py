import subprocess
import time
import logging
import os
from datetime import datetime
from typing import List, Dict, Union, Optional
from dataclasses import dataclass
from pathlib import Path

@dataclass
class Job:
    name: str
    command: str
    working_dir: str
    status: str = "pending"
    start_time: float = None
    end_time: float = None
    depends_on: Optional[Union[str, List[str]]] = None
    
class JobManager:
    def __init__(self, max_concurrent_jobs: int = 2):
        self.max_concurrent_jobs = max_concurrent_jobs
        self.jobs: List[Job] = []
        self.running_jobs: Dict[str, subprocess.Popen] = {}
        self.completed_jobs: List[str] = []
        
        # Setup logging
        self.setup_logging()
    
    def setup_logging(self):
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_dir / f"job_manager_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
                logging.StreamHandler()
            ]
        )
    
    def add_job(self, name: str, command: str, working_dir: str = ".", depends_on: Union[str, List[str]] = None) -> str:
        """Add a new job to the queue"""
        job = Job(name=name, command=command, working_dir=working_dir, depends_on=depends_on)
        self.jobs.append(job)
        logging.info(f"Added job: {name}" + (f" with dependencies: {depends_on}" if depends_on else ""))
        return name  # Return job name for dependency reference
    
    def check_dependencies(self, job: Job) -> bool:
        """Check if all dependencies for a job are completed successfully"""
        if not job.depends_on:
            return True
            
        dependencies = [job.depends_on] if isinstance(job.depends_on, str) else job.depends_on
        return all(dep in self.completed_jobs for dep in dependencies)
    
    def start_job(self, job: Job):
        """Start a specific job"""
        try:
            process = subprocess.Popen(
                job.command,
                shell=True,
                cwd=job.working_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            job.status = "running"
            job.start_time = time.time()
            self.running_jobs[job.name] = process
            logging.info(f"Started job: {job.name}")
            
        except Exception as e:
            logging.error(f"Error starting job {job.name}: {str(e)}")
            job.status = "failed"
    
    def check_running_jobs(self):
        """Check status of running jobs and update accordingly"""
        completed_jobs = []
        
        for job_name, process in self.running_jobs.items():
            if process.poll() is not None:  # Job has finished
                job = next(j for j in self.jobs if j.name == job_name)
                job.end_time = time.time()
                job.status = "completed" if process.returncode == 0 else "failed"
                
                stdout, stderr = process.communicate()
                logging.info(f"Job {job_name} completed with status: {job.status}")
                if stderr:
                    logging.error(f"Job {job_name} errors: {stderr}")
                
                if job.status == "completed":
                    self.completed_jobs.append(job_name)
                completed_jobs.append(job_name)
        
        # Remove completed jobs from running_jobs
        for job_name in completed_jobs:
            del self.running_jobs[job_name]
    
    def get_next_job(self) -> Optional[Job]:
        """Get the next job that is ready to run (pending and dependencies met)"""
        for job in self.jobs:
            if job.status == "pending" and self.check_dependencies(job):
                return job
        return None
    
    def run_jobs(self):
        """Main method to run and manage jobs"""
        while (self.running_jobs or 
               any(job.status == "pending" for job in self.jobs)):
            # Check running jobs
            self.check_running_jobs()
            
            # Start new jobs if possible
            while (len(self.running_jobs) < self.max_concurrent_jobs):
                next_job = self.get_next_job()
                if next_job:
                    self.start_job(next_job)
                else:
                    break  # No jobs ready to run
            
            time.sleep(1)  # Prevent CPU overuse
            
        logging.info("All jobs completed")
        
        # Print final status
        for job in self.jobs:
            logging.info(f"Job {job.name} final status: {job.status}") 