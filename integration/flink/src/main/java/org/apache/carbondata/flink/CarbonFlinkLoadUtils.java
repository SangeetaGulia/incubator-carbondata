package org.apache.carbondata.flink;

import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;

import java.util.Arrays;
import java.util.List;

public class CarbonFlinkLoadUtils {

    public int getLoadCount(String tableFolderPath) {
        int currentLoadCount = -1;
        LoadMetadataDetails[] loadMetadataDetails = SegmentStatusManager.readLoadMetadata(tableFolderPath);
        List<LoadMetadataDetails> metadataDetails = Arrays.asList(loadMetadataDetails);
        if (metadataDetails.size() != 0) {
            for (LoadMetadataDetails metaDetail : metadataDetails) {
                int loadCount = 0;
                try {
                    loadCount = Integer.parseInt(metaDetail.getLoadName());
                } catch (NumberFormatException exp) {
                    break;
                }
                if (currentLoadCount < loadCount) {
                    currentLoadCount = loadCount;
                }
            }
        }
        currentLoadCount++;
        return currentLoadCount;
    }


}
