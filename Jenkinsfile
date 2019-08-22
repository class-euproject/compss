pipeline {
    agent {
        dockerfile {
            filename "Dockerfile"
            args "--privileged"
        }
    }

    stages {
        /*
        stage("Git pull") {
            steps {
                git branch: 'container-integration',
                    credentialsId: '6a3a04da-0458-4a30-b2ab-5d6e1ac2012a',
                    url: 'https://gitlab.bsc.es/ppc/software/compss.git'
            }
        }
        stage ("Docker build") {
            steps {
                script {
                    compssEnv = docker.build("jenkins-compss:${env.BUILD_ID}")
                }
            }
        }
        */
        stage("Compiling") {
            steps {
                script {
                    dir "/root/framework/builders"
                    sh "./buildlocal -M -B -P -T -A -K -i /opt/COMPSs"
                }
            }
        }
    }

}