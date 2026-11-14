package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ColumnFamilyDescriptorTest {
  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void newInstance() throws RocksDBException {
    try (final ColumnFamilyDescriptor cf_descriptor =
             new ColumnFamilyDescriptor("new_CF".getBytes(StandardCharsets.UTF_8))) {
      assertThat(cf_descriptor.getName()).isEqualTo("new_CF".getBytes(StandardCharsets.UTF_8));
      assertThat(cf_descriptor.getOptions()).isNotNull();
      assertThat(cf_descriptor.getOptions().nativeHandle_).isNotEqualTo(0l);
    }
  }

  @Test
  public void fromHandle2Descriptor() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath());
        final ColumnFamilyDescriptor cfDescriptor =
            new ColumnFamilyDescriptor("myCf".getBytes(StandardCharsets.UTF_8))) {
      final ColumnFamilyHandle cfHandle = db.createColumnFamily(cfDescriptor);

      final ColumnFamilyDescriptor result =
          cfHandle.getDescriptor(); // This creates new ColumnFamilyDescriptor in JNI code.
                                    //   instance of ColumnFamilyOptions is created with underlying
                                    //   C++ allocation and ColumnFamilyDescriptor must free this
                                    //   memory.

      assertThat(result).isNotNull();

      final ColumnFamilyOptions cfOptions = result.getOptions();
      assertThat(cfOptions.isOwningHandle()).isTrue();

      result.close(); // Closing  ColumnFamilyDescriptor must
      assertThat(cfOptions.isOwningHandle())
          .isFalse(); // close underlying cfOptions even when instance is created in JNI code.
    }
  }

  @Test
  public void closeTransitiveResources() throws RocksDBException {
    final ColumnFamilyDescriptor cfDescriptor =
        new ColumnFamilyDescriptor("myCf".getBytes(StandardCharsets.UTF_8));
    assertThat(cfDescriptor.isOwningHandle()).isTrue();

    final ColumnFamilyOptions cfOptions = cfDescriptor.getOptions();

    assertThat(cfOptions.isOwningHandle()).isTrue();

    cfDescriptor.close();

    assertThat(cfDescriptor.isOwningHandle()).isFalse();
    assertThat(cfOptions.isOwningHandle()).isFalse();
  }
}
