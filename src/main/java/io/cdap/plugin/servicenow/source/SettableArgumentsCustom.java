package io.cdap.plugin.servicenow.source;

import io.cdap.cdap.etl.api.action.SettableArguments;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SettableArgumentsCustom implements SettableArguments {

    private final Map<String, String> settableArgumentsMap;

    SettableArgumentsCustom() {
        this.settableArgumentsMap = new HashMap<>();
    }

    @Override
    public boolean has(String name) {
        return this.settableArgumentsMap.containsKey(name);
    }

    @Nullable
    @Override
    public String get(String name) {
        if (this.has(name)) {
            return this.settableArgumentsMap.get(name);
        }
        return null;
    }

    @Override
    public void set(String name, String value) {
        this.settableArgumentsMap.put(name, value);
    }

    @Override
    public Map<String, String> asMap() {
        return this.settableArgumentsMap;
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
        return this.settableArgumentsMap.entrySet().iterator();
    }
}
