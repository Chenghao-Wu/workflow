# workflow

A Python package for managing and automating SLURM job workflows.

## Features

- Submit and manage SLURM jobs programmatically
- Handle job dependencies
- Monitor job status automatically
- Control concurrent job limits
- Configurable SLURM parameters

## Installation

## Usage

### Basic Example

Here's a simple example of submitting and managing SLURM jobs:

### Job Dependencies

You can create workflows with job dependencies:

## Supported SLURM Parameters

The `add_job()` method supports the following SLURM parameters:

- `name`: Job name
- `script_path`: Path to the job script
- `working_dir`: Working directory for the job (default: ".")
- `partition`: SLURM partition
- `memory`: Memory requirement (e.g., "2G")
- `time_limit`: Time limit (format: "HH:MM:SS")
- `nodes`: Number of nodes
- `ntasks`: Number of tasks
- `cpus_per_task`: CPUs per task
- `qos`: Quality of Service
- `depends_on`: Job dependencies (single job ID or list of job IDs)

## Output and Logging

- Job outputs are stored in `slurm_logs/{job_name}_{job_id}.out`
- Error logs are stored in `slurm_logs/{job_name}_{job_id}.err`
- The package uses Python's logging module to provide execution information
- Job status updates are logged at INFO level

Example log output:

## Examples

Check the `workflow/examples` directory for more detailed examples, including:
- `slurm_example.py`: Demonstrates job submission with dependencies

## Requirements

- Python 3.6+
- Access to a SLURM cluster
- SLURM commands (`sbatch`, `sacct`) available in PATH

## Error Handling

The package includes robust error handling for common SLURM-related issues:
- Job submission failures
- Invalid parameters
- Dependency resolution problems
- Job status monitoring errors

## License

[Add your license information here]

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

For support, please [create an issue](repository-issues-url) on the GitHub repository.
