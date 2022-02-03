package io.cdap.plugin.servicenow.source;

import java.io.Serializable;

public class MyRecord implements Serializable {
    public String input;

    MyRecord(String input) {
        this.input = input;
    }

}
