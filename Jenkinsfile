#!groovy

pipeline {
    agent any
    
    stages {
        stage('Checkout') {
            steps {
                checkout scm
                sh 'mkdir -p build local'
            }
        }
        stage('Compile') {
            steps {
                sh '''
                	cd build &&
                	cmake -D CMAKE_BUILD_TYPE=Debug -D BPTREE_BUILD_TESTS=ON .. &&
                	make
                '''
            }
        }
        stage('Test') {
            steps {
                sh '''
                	cd build &&
                	make test
                '''
            }
        }
        stage('Package') {
            steps {
                sh '''
                	cd build &&
                	cmake -D CMAKE_BUILD_TYPE=Release -D BPTREE_BUILD_TESTS=OFF -D CMAKE_INSTALL_PREFIX:PATH=$PWD/../local ..
                	make
                	make install
                '''
				archiveArtifacts artifacts: 'local/**', fingerprint: true, onlyIfSuccessful: true
            }
        }
    }
}