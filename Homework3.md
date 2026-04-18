make init -> 
    This command copies the .env.example file that has been created in our repository (e.g passwords, ports, usernames etc) and init command uses that example file to copy the actual wanted result to .env file that we are going to use in our project.
    It creates a specifically named docker network connection so that containers can communicate between eachother. All running services are connected with the created connection.
    This command must be run before starting the project.

    Makefile and env.example files must exist before running make init - Makefile must know what init means and env.example must exist to be able to copy the information from.
    The command make takes it's actions from Makefile, where the specific commands have been described more thorougly on what every action specifically does.
    Makefile is needed to make the workflow easier, so that you would not have to write long commands in order to start dev containers and working on your project.

make up-superset -> 
    This command starts the superset stack, that has been described in our repository.
    If we run this command, we can now enter superset via our local computer.
    For superset to run, we need to have our database and metadata database (PostgreSQL and postgres) and docker. 

    Postgres -> This is a tool, that saves metadata in our database
    Superset -> Open-Source business intelligence application
    Superset-redis -> This is used for caching data in memory within its container.
    Superset-init -> This is a task that sets the superset up and then exits. Only used once to bring the superset up. Running it and then seeing it exit is expected

make devcontainer-join-course-network ->
    This action is needed to connect the devcontainer that we have started to be able to connect it with superset stack.
    This connects our devcontainer (where we write code) with superset stack (hosted by docker and holding services like postgres, redis, superset etc)
    If we run ETL command without connecting these two containers, the connection would fail because the devcontainer has no network route to reach superset stack.

make ps ->
    Running this command shows us which containers are running, which services they are using, what is their statuses and what ports they are using

How make and Docker compose work together ->
    As described before, then Makefile is used to make the workflow easier. Whatever is described in Makefile, you can describe them once and then use short codes to call out longer blocks of code whenever you need them.
    Make command calls out Compose (Docker compose) file (docker-compose.yml), that will manage the lifecycles in containers. docker-compose.yml file contains different profiles, that can be called out with Makefile (if described in them)

Use case 1 - the Superset stack already exists but is stopped;
    You just start the Superset stack again by using the make up-superset, make devcontainer-join-course-network commands and control, that everything works as you wish. As no-one had removed the containers, all data should still exist and run as needed.

Use case 2 - the containers have been removed;
    When containers have been removed, then you can still use the same commands as described in use case 1, but the difference is that then the devcontainers are re-created from docker compose file and the set up takes a bit more time. The superset-init command (starts, set up superset, exits) must set up the connection again and all data would be also lost, if volumes were deleted. Make init would not be needed in that case.

Use case 3 - you changed .env values or reopened the repository from a different host folder.
    When reopened repository from another host folder, then also make init command is needed and then you can run the make up-superset command. This use case is kind of like full-restart, where you need to re-create and re-connect the containers and then start working on your project. If some old containers were left running, you must first shut down old container via docker compose down and then re-start containers again (if some of the old containers had old information saved on them)