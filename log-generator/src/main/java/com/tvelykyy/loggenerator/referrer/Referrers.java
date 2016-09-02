package com.tvelykyy.loggenerator.referrer;

import java.util.ArrayList;
import java.util.List;

/**
 * Class generates collections of referrers.
 */
public class Referrers {

    private List<String> refs = new ArrayList<>();

    public Referrers() {
        refs.add("http://google.com/search");
        refs.add("http://google.com/webhp");
        refs.add("http://yahoo.com/search");
        refs.add("http://bing.com/search.php");
        refs.add("http://gigablast.com/");
    }

    /**
     * Returns referrer by index.
     */
    public String getReferrer(int index) {
        return refs.get(index);
    }

    /**
     * Returns number of generated referrers.
     */
    public int getCount() {
        return refs.size();
    }
}
