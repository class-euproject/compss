package es.bsc.compss.apps.hello;

public class Hello {

    public static void main(String [] args) throws Exception {
        System.out.println("Hola, món! (Des de l'aplicació principal)");

        HelloImpl.sayHello();
    }

}
