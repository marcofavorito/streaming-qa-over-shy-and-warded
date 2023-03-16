# Run experiments on the Cloud

## Setup

- you might need to install a new drive on the VM. 
  The following guide is a good cheatsheet of commands to run
  in order to mount additional storage disks easily: https://help.ubuntu.com/community/InstallingANewHardDrive

- to mount the disk, first make sure you know the name of the partition using
  `lsblk`; then use this command to mount it (assuming the partition is `/dev/sdb1`):
```
sudo mount -t ext4 /dev/sdb1 disk/
```

- If not formatted, run: `sudo mkfs -t ext4 /dev/sdb`

- Other needed dependencies:

```
sudo apt-get install python3-pip python3-dev
```

- remember to use `tmux` to launch the experiments!
- remember to put the output directory of the scripts to the dicsk


## Run

TODO

## Copy from remote VM

Instead of copying all the data and calibratino checkpoints,
it might be useful to follow these commands so to copy only specific files,
namely `calibration_results.csv`, `benchmark-config.yml`,
`config.yml` and `output.log`:

- on remote machine, get all CSV files: 
```
find chasebench | grep -E "output.tsv|output.log|vadalog-output.log" > files-to-copy.txt
```

- on local machine, copy the `files-to-copy.txt` file:
```
scp -r datalogbench:"/home/datalog-benchmarking/disk/results/files-to-copy.txt" ./
```

- on local machine, create directories to store files:
```
cat files-to-copy.txt | xargs dirname | xargs mkdir -p
```

- on local machine, copy files from remote in the right location:
```
cat files-to-copy.txt | xargs -I{} scp datalogbench:/home/datalog-benchmarking/disk/results/{} ./{}
```
