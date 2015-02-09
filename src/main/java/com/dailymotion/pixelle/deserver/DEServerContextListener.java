package com.dailymotion.pixelle.deserver; /**
 * Created by n.dhupia on 10/29/14.
 */


import com.dailymotion.pixelle.deserver.servlets.DeServletModule;
import com.google.inject.Module;
import com.squarespace.jersey2.guice.JerseyGuiceServletContextListener;

import java.util.Arrays;
import java.util.List;

public class DEServerContextListener extends JerseyGuiceServletContextListener {
    @Override
    protected List<? extends Module> modules() {
        return Arrays.asList(new DeServletModule());
    }
}