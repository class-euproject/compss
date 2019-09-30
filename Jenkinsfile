pipeline {
    agent {
        dockerfile {
            filename "Dockerfile"
            additionalBuildArgs  '-t bsc-ppc/compss-docker-test'
            args "--privileged -e DOCKER_HOST=unix:///var/run/docker.sock -u root:root -v /home/`whoami`/.m2/repository:/root/.m2"
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
                    sh "/root/framework/builders/buildlocal -M -B -P -T -A -K -X -d /opt/COMPSs"
                }
            }
        }
        stage("Building app and its images") {
            steps {
                script {
                    sh "echo " + pwd()
                    sh "mvn -f /root/framework/tests/containers/pom.xml -DskipTests " +
                            "clean package exec:exec@genimage-docker"
                }
            }
        }
        stage("Testing") {
            steps {
                script {
                    sh "cd /root/framework/tests/containers && mvn test"
                }
            }
        }
    }

    post {
        failure {
            updateGitlabCommitStatus name: 'Compiling', state: 'failed'
            emailext attachLog: true,
                    body: "<b>An error has ocurred</b><br>Project: ${env.JOB_NAME} #${env.BUILD_NUMBER} <br/>" +
                            "URL:  <a href='${env.BUILD_URL}'>${env.BUILD_URL}</a>",
                    subject: "ERROR ON ${env.JOB_NAME} #${env.BUILD_NUMBER}",
                    to: 'unai.perez@bsc.es'
        }
        success {
            junit "/root/framework/tests/containers/target/surefire-reports/*.xml"
            updateGitlabCommitStatus name: 'Compiling', state: 'success'
        }
        always{
            deleteDir()
            // sh "docker rmi bsc-ppc/compss-docker-test -f"
        }
    }

    options {
        gitLabConnection('Gitlab')
    }

}