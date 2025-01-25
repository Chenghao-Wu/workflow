import sys
sys.path.append('..')
from workflow import JobManager

def main():
    manager = JobManager(max_concurrent_jobs=2)
    
    # Add first job
    job1_id = manager.add_job(
        name="job1",
        command="python -c 'import time; print(\"Job 1 starting\"); time.sleep(3); print(\"Job 1 done\")'",
        working_dir="."
    )
    
    # Add second job (depends on job1)
    job2_id = manager.add_job(
        name="job2",
        command="python -c 'import time; print(\"Job 2 starting\"); time.sleep(2); print(\"Job 2 done\")'",
        working_dir=".",
        depends_on=job1_id  # This job will start only after job1 completes
    )
    
    # Add third job (depends on both job1 and job2)
    job3_id = manager.add_job(
        name="job3",
        command="python -c 'import time; print(\"Job 3 starting\"); time.sleep(1); print(\"Job 3 done\")'",
        working_dir=".",
        depends_on=[job1_id, job2_id]  # This job will start only after both job1 and job2 complete
    )
    
    print("Submitted jobs with IDs:", job1_id, job2_id, job3_id)
    print("Job dependencies:")
    print(f"Job 2 depends on Job 1")
    print(f"Job 3 depends on Job 1 and Job 2")
    
    # Run all jobs
    manager.run_jobs()

if __name__ == "__main__":
    main() 