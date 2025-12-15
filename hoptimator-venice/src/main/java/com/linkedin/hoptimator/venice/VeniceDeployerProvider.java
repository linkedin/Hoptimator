package com.linkedin.hoptimator.venice;

import com.linkedin.hoptimator.Deployable;
import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.DeployerProvider;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


public class VeniceDeployerProvider implements DeployerProvider {

    @Override
    public <T extends Deployable> Collection<Deployer> deployers(T obj, Connection connection) {
        List<Deployer> list = new ArrayList<>();
        if (obj instanceof Source) {
            Source source = (Source) obj;
            if (source.database().equalsIgnoreCase(VeniceDriver.CATALOG_NAME)) {
                list.add(new VeniceDeployer(source, (HoptimatorConnection) connection));
            }
        }

        return list;
    }

    @Override
    public int priority() {
        return 2;
    }
}
