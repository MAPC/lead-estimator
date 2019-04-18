# LEAD Estimator

This program generates the datasets used by the [LEAD website](https://lead.mapc.org).

#### Dependencies
- Python3
- Python packages listed in the Dockerfile

#### Environment
You need to enter in the database information into an env file.

```sh
cp .env.sample .env && vim .env
```

### Usage
```sh
docker-compose up
```

There are a couple of commandline arguments which are good for development but not
necessary for running the program as desired. You can view these arguments in the _estimate.py_ file.

The only argument worth knowing is `--push`. The push flag tells the program to publish the 
generated dataset to the database which is pulled the source data from.
**`--push` should not be used while developing.**
