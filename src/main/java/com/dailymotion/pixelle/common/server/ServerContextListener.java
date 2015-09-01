package com.dailymotion.pixelle.common.server; /**
 * Created by n.dhupia on 10/29/14.
 */


import com.google.inject.Module;
import com.squarespace.jersey2.guice.JerseyGuiceServletContextListener;

import java.util.Arrays;
import java.util.List;

class ServerContextListener extends JerseyGuiceServletContextListener {
    @Override
    protected List<? extends Module> modules() {
        return Arrays.asList(new AppServletModule());
    }
}
