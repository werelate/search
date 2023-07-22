package org.werelate.test;

import java.util.List;

import org.folg.names.search.Normalizer;

public class Test
{
  public static void main(String[] args) {
    Normalizer normalizer = Normalizer.getInstance();
    List<String> tokens = normalizer.normalize("james earl", false);
    for (String token : tokens) {
      System.out.println(token);
    }
  }
}
