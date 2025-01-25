import sys
sys.path.append('..')
from workflow import SlurmJobManager
import os

def create_test_scripts():
    """Create test scripts for the example"""
    
    # Create first job script
    with open("job1.sh", "w") as f:
        f.write("""#!/bin/bash
echo "Starting Job 1"
sleep 30
echo "Job 1 completed"
""")
    
    # Create second job script
    with open("job2.sh", "w") as f:
        f.write("""#!/bin/bash
echo "Starting Job 2"
sleep 20
echo "Job 2 completed"
""")
    
    # Create third job script
    with open("job3.sh", "w") as f:
        f.write("""#!/bin/bash
echo "Starting Job 3"
sleep 10
echo "Job 3 completed"
""")
    
    # Make scripts executable
    os.chmod("job1.sh", 0o755)
    os.chmod("job2.sh", 0o755)
    os.chmod("job3.sh", 0o755)

def main():
    # Create test scripts
    create_test_scripts()
    
    # Initialize Slurm manager
    manager = SlurmJobManager(max_concurrent_jobs=5)
    
    # Submit first job
    job1_id = manager.add_job(
        name="test_job1",
        script_path="./job1.sh",
        working_dir=".",
        partition="cpu6348",
        memory="2G",
        time_limit="00:10:00",
        qos='78cores'
    )
    
    # Submit second job (depends on job1)
    job2_id = manager.add_job(
        name="test_job2",
        script_path="./job2.sh",
        working_dir=".",
        partition="cpu6348",
        memory="2G",
        time_limit="00:10:00",
        qos='78cores',
        depends_on=job1_id  # This job will start only after job1 completes
    )
    
    # Submit third job (depends on both job1 and job2)
    job3_id = manager.add_job(
        name="test_job3",
        script_path="./job3.sh",
        working_dir=".",
        partition="cpu6348",
        memory="2G",
        time_limit="00:10:00",
        qos='78cores',
        depends_on=[job1_id, job2_id]  # This job will start only after both job1 and job2 complete
    )
    
    print(f"Submitted jobs with IDs: {job1_id}, {job2_id}, {job3_id}")
    print("Job dependencies:")
    print(f"Job 2 depends on Job 1")
    print(f"Job 3 depends on Job 1 and Job 2")
    
    # Run all jobs
    manager.run_jobs()

if __name__ == "__main__":
    main() 