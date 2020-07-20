pipeline {
    agent any

    tools {
        // Install the Maven version configured as "M3" and add it to the path.
        maven "maven"
    }

    stages {
        stage('Git pull') {
            steps {
                git branch: 'ppc/ilp-cloudprovider-merge',
                    credentialsId: 'ef0a7d37-3ca2-42e7-a953-36d2110d7f98',
                    url: 'https://gitlab.bsc.es/ppc/software/compss'
            }
        }
        stage('Build runtime') {
            steps {
                sh "mvn clean package -DskipTests"
            }
        }
        stage('Build COMPSs Java worker image') {
            steps {
                script {
                    // def conn_version = sh(returnStdout: true, script: "mvn -q -N -Dexec.executable='echo' -f \"${WORKSPACE}/compss\" -Dexec.args='${conn.version}' org.codehaus.mojo:exec-maven-plugin:1.3.1:exec").trim()
                    def version = sh(returnStdout: true, script: "mvn -q -N -Dexec.executable='echo' -Dexec.args='\${project.version}' org.codehaus.mojo:exec-maven-plugin:1.3.1:exec").trim()
                    def arch = sh(returnStdout: true, script: "arch").trim()
                    def java_image = docker.build("bscppc/compss-worker-${arch}:${version}", "-f dockerfiles/Dockerfile_base_compss .")
                    docker.withRegistry('https://index.docker.io/v1/', 'unai-docker-hub') {
                        java_image.push("latest")
                    }
                }
            }
        }
        stage('Build COMPSs Python worker image') {
            steps {
                script {
                    def conn_version = sh(returnStdout: true, script: "mvn -q -N -Dexec.executable='echo' -f \"${WORKSPACE}/compss\" -Dexec.args='\${conn.version}' org.codehaus.mojo:exec-maven-plugin:1.3.1:exec").trim()
                    def version = sh(returnStdout: true, script: "mvn -q -N -Dexec.executable='echo' -Dexec.args='\${project.version}' org.codehaus.mojo:exec-maven-plugin:1.3.1:exec").trim()
                    def arch = sh(returnStdout: true, script: "arch").trim()
                    def python_image = docker.build("bscppc/pycompss-worker-${arch}:${version}", "--build-arg CONN_VERSION=${conn_version} -f dockerfiles/Dockerfile_base_pycompss .")
                    docker.withRegistry('https://index.docker.io/v1/', 'unai-docker-hub') {
                        python_image.push("latest")
                    }
                }
            }
        }
        stage('Build COMPSs Java image') {
            steps {
                script {
                    def conn_version = sh(returnStdout: true, script: "mvn -q -N -Dexec.executable='echo' -f \"${WORKSPACE}/compss\" -Dexec.args='\${conn.version}' org.codehaus.mojo:exec-maven-plugin:1.3.1:exec").trim()
                    def version = sh(returnStdout: true, script: "mvn -q -N -Dexec.executable='echo' -Dexec.args='\${project.version}' org.codehaus.mojo:exec-maven-plugin:1.3.1:exec").trim()
                    def arch = sh(returnStdout: true, script: "arch").trim()
                    def compss_java_image = docker.build("bscppc/compss:${version}", "--build-arg CONN_VERSION=${conn_version} -f dockerfiles/Dockerfile_compss .")
                    docker.withRegistry('https://index.docker.io/v1/', 'unai-docker-hub') {
                        compss_java_image.push("latest")
                    }
                }
            }
        }
        stage('Build COMPSs Python image') {
            steps {
                script {
                    def conn_version = sh(returnStdout: true, script: "mvn -q -N -Dexec.executable='echo' -f \"${WORKSPACE}/compss\" -Dexec.args='\${conn.version}' org.codehaus.mojo:exec-maven-plugin:1.3.1:exec").trim()
                    def version = sh(returnStdout: true, script: "mvn -q -N -Dexec.executable='echo' -Dexec.args='\${project.version}' org.codehaus.mojo:exec-maven-plugin:1.3.1:exec").trim()
                    def arch = sh(returnStdout: true, script: "arch").trim()
                    def compss_python_image = docker.build("bscppc/pycompss:${version}", "--build-arg CONN_VERSION=${conn_version} -f dockerfiles/Dockerfile_pycompss .")
                    docker.withRegistry('https://index.docker.io/v1/', 'unai-docker-hub') {
                        compss_python_image.push("latest")
                    }
                }
            }
        }
        /* TODO: Docker manifest */
        /* TODO: Multiarch build */
    }
    post {
        success {
            updateGitlabCommitStatus name: 'jenkins', state: 'success'
        }
        failure {
            updateGitlabCommitStatus name: 'jenkins', state: 'failed'
        }
    }
}
