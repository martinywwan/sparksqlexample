package com.martinywwan.model;

import java.io.Serializable;

public class Person implements Serializable {

    private String name;

    private Integer age;

    private int version;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    @Override
    public String toString(){
//            System.out.println("HELLO : " + name);
        return "HELLO : " + name;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getVersion() {
        return version;
    }
}