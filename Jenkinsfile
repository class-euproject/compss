pipeline {
    agent {
        dockerfile {
            filename "Dockerfile"
            args "--privileged -e DOCKER_HOST=unix:///var/run/docker.sock -u root:root -v /home/`whoami`/.m2/repository:/home/root/repository"
        }
    }

    stages {
        stage("Environment setup") {
            steps {
                script {
                    sh "nohup lxd &"
                    sh "nohup dockerd &"
                }
            }
        }
        stage("Compiling") {
            steps {
                script {
                    sh "/root/framework/builders/buildlocal -M -B -P -T -A -K -i /opt/COMPSs"
                }
            }
        }
    }

    post {
        success {
            setBuildStatus("Build succeeded", "SUCCESS");
        }
        failure {
            setBuildStatus("Build failed", "FAILURE");
        }
    }

}