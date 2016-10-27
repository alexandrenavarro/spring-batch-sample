package hello;

/**
 * Created by anavarro on 27/10/16.
 */
//@RestController
public class HelloController {

    //@RequestMapping("/")
    public String index() {
        return "Greetings from Spring Boot!";
    }

}