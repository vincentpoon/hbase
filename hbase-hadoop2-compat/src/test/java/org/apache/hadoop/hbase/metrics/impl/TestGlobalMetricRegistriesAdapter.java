package org.apache.hadoop.hbase.metrics.impl;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.hadoop.hbase.metrics.MetricRegistryInfo;
import org.apache.hadoop.hbase.testclassification.MetricsTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({ MetricsTests.class, SmallTests.class })
public class TestGlobalMetricRegistriesAdapter {

  /**
   * Tests that using reflection to unregister the Hadoop metrics source works properly
   */
  @Test
  public void testUnregisterSource() {
    GlobalMetricRegistriesAdapter adapter = GlobalMetricRegistriesAdapter.init();
    // we'll configure the sources manually, so disable the executor
    adapter.stop();
    TestSource ts1 = new TestSource("ts1");
    TestSource ts2 = new TestSource("ts2");
    MetricsSystem metricsSystem = DefaultMetricsSystem.instance();
    metricsSystem.register("ts1", "", ts1);
    metricsSystem.register("ts2", "", ts2);
    MetricsSource s1 = metricsSystem.getSource("ts1");
    assertNotNull(s1);
    MetricRegistryInfo mockRegistryInfo = Mockito.mock(MetricRegistryInfo.class);
    Mockito.when(mockRegistryInfo.getMetricsJmxContext()).thenReturn("ts1");
    adapter.unregisterSource(mockRegistryInfo);
    s1 = metricsSystem.getSource("ts1");
    assertNull(s1);
    MetricsSource s2 = metricsSystem.getSource("ts2");
    assertNotNull(s2);
  }

  @Metrics(context = "test")
  private static class TestSource {
    @Metric("C1 desc")
    MutableCounterLong c1;
    @Metric("XXX desc")
    MutableCounterLong xxx;
    @Metric("G1 desc")
    MutableGaugeLong g1;
    @Metric("YYY desc")
    MutableGaugeLong yyy;
    @Metric
    MutableRate s1;
    @SuppressWarnings("unused")
    final MetricsRegistry registry;

    TestSource(String recName) {
      registry = new MetricsRegistry(recName);
    }
  }

}
