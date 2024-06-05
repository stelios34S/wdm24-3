# Web-scale Data Management Project Template

Basic project structure with Python's Flask and Redis. RabbitMQ used for message exchange.
The orchestrator acts as the gateway for all HTTP requests and is responsible for providing the appropriate responses. It also
sends the appropriate messages through RabbitMQ channels to the order, payment, and stock services and waits for the appropriate
ACKs. The services accept and publish messages exclusively, never HTTP requests. The transactional protocol and message exchange
is modeled after a choreography based SAGA architecture.

### Project structure

* `env`
    Folder containing the Redis env variables for the docker-compose deployment
    
* `helm-config` 
    Helm chart values for Redis and ingress-nginx

* `orchestrator`
    Folder containing the orchestrator logic and dockerfile.

* `order`
    Folder containing the order application logic and dockerfile. 
    
* `payment`
    Folder containing the payment application logic and dockerfile. 

* `stock`
    Folder containing the stock application logic and dockerfile. 

* `test`
    Folder containing some basic correctness tests for the entire system.

### Deployment types:

#### docker-compose (local development)

After coding the REST endpoint logic run `docker-compose up --build` in the base folder to test if your logic is correct
(you can use the provided tests in the `\test` folder and change them as you wish). 

***Requirements:*** You need to have docker and docker-compose installed on your machine.

### Consistency test

In order for the consistency test to run on our codebase, it is necessary to change the `urls.json` file to the one 
provided at the top level of this repository. This is due to the fact that the orchestrator server is the 'gateway' of our system
hence all HTTP requests must go through there.