package com.martinywwan.model;

import java.io.Serializable;

public class Person implements Serializable {

    private String id;

    private String namePerson;

    private Integer age;

    private int versionNumber;

    public String getNamePerson() {
        return namePerson;
    }

    public void setNamePerson(String namePerson) {
        this.namePerson = namePerson;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    @Override
    public String toString(){
//            System.out.println("HELLO : " + namePerson);
        return "HELLO : " + namePerson;
    }

    public void setVersionNumber(int versionNumber) {
        this.versionNumber = versionNumber;
    }

    public int getVersionNumber() {
        return versionNumber;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}