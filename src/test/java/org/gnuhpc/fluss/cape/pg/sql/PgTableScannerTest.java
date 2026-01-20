package org.gnuhpc.fluss.cape.pg.sql;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.Scan;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PgTableScannerTest {

    @Mock
    private Connection mockConnection;

    @Mock
    private Table mockTable;

    @Mock
    private TableInfo mockTableInfo;

    @Mock
    private Scan mockScan;

    @Mock
    private LogScanner mockScanner;

    @Mock
    private ScanRecords mockScanRecords;

    private RowType testRowType;

    @BeforeEach
    void setup() throws Exception {
        testRowType = RowType.of(
            DataTypes.INT(),
            DataTypes.STRING(),
            DataTypes.INT()
        );
        
        lenient().when(mockTable.getTableInfo()).thenReturn(mockTableInfo);
        lenient().when(mockTableInfo.getNumBuckets()).thenReturn(3);
        lenient().when(mockTable.newScan()).thenReturn(mockScan);
        lenient().when(mockScan.createLogScanner()).thenReturn(mockScanner);
        lenient().when(mockScan.project(anyList())).thenReturn(mockScan);
        lenient().doNothing().when(mockScanner).subscribeFromBeginning(anyInt());
        lenient().doNothing().when(mockScanner).close();
    }

    @Test
    void testScanTableWithNoData() throws Exception {
        when(mockScanner.poll(any(Duration.class))).thenReturn(mockScanRecords);
        when(mockScanRecords.count()).thenReturn(0);

        List<InternalRow> result = PgTableScanner.scanTable(mockConnection, mockTable, null, -1);

        assertThat(result).isEmpty();
        verify(mockScanner, times(3)).subscribeFromBeginning(anyInt());
        verify(mockScanner).close();
    }

    @Test
    void testScanTableWithSingleRow() throws Exception {
        InternalRow mockRow = mock(InternalRow.class);
        ScanRecord mockRecord = mock(ScanRecord.class);
        when(mockRecord.getRow()).thenReturn(mockRow);

        TableBucket bucket = new TableBucket(0L, 0);
        
        when(mockScanRecords.count()).thenReturn(1).thenReturn(0);
        when(mockScanRecords.buckets()).thenReturn(Collections.singleton(bucket));
        when(mockScanRecords.records(bucket)).thenReturn(Collections.singletonList(mockRecord));
        when(mockScanner.poll(any(Duration.class))).thenReturn(mockScanRecords);

        List<InternalRow> result = PgTableScanner.scanTable(mockConnection, mockTable, null, -1);

        assertThat(result).hasSize(1);
        assertThat(result.get(0)).isEqualTo(mockRow);
        verify(mockScanner).close();
    }

    @Test
    void testScanTableWithMultipleRows() throws Exception {
        InternalRow mockRow1 = mock(InternalRow.class);
        InternalRow mockRow2 = mock(InternalRow.class);
        InternalRow mockRow3 = mock(InternalRow.class);

        ScanRecord mockRecord1 = mock(ScanRecord.class);
        ScanRecord mockRecord2 = mock(ScanRecord.class);
        ScanRecord mockRecord3 = mock(ScanRecord.class);

        when(mockRecord1.getRow()).thenReturn(mockRow1);
        when(mockRecord2.getRow()).thenReturn(mockRow2);
        when(mockRecord3.getRow()).thenReturn(mockRow3);

        TableBucket bucket1 = new TableBucket(0L, 0);
        TableBucket bucket2 = new TableBucket(0L, 1);

        when(mockScanRecords.count())
            .thenReturn(2)
            .thenReturn(1)
            .thenReturn(0);
        
        when(mockScanRecords.buckets())
            .thenReturn(Collections.singleton(bucket1))
            .thenReturn(Collections.singleton(bucket2));
        
        when(mockScanRecords.records(bucket1))
            .thenReturn(Arrays.asList(mockRecord1, mockRecord2));
        
        when(mockScanRecords.records(bucket2))
            .thenReturn(Collections.singletonList(mockRecord3));

        when(mockScanner.poll(any(Duration.class))).thenReturn(mockScanRecords);

        List<InternalRow> result = PgTableScanner.scanTable(mockConnection, mockTable, null, -1);

        assertThat(result).hasSize(3);
        assertThat(result).containsExactly(mockRow1, mockRow2, mockRow3);
        verify(mockScanner).close();
    }

    @Test
    void testScanTableWithLimit() throws Exception {
        InternalRow mockRow1 = mock(InternalRow.class);
        InternalRow mockRow2 = mock(InternalRow.class);
        InternalRow mockRow3 = mock(InternalRow.class);

        ScanRecord mockRecord1 = mock(ScanRecord.class);
        ScanRecord mockRecord2 = mock(ScanRecord.class);
        ScanRecord mockRecord3 = mock(ScanRecord.class);

        when(mockRecord1.getRow()).thenReturn(mockRow1);
        when(mockRecord2.getRow()).thenReturn(mockRow2);

        TableBucket bucket = new TableBucket(0L, 0);

        when(mockScanRecords.count()).thenReturn(3).thenReturn(0);
        when(mockScanRecords.buckets()).thenReturn(Collections.singleton(bucket));
        when(mockScanRecords.records(bucket))
            .thenReturn(Arrays.asList(mockRecord1, mockRecord2, mockRecord3));

        when(mockScanner.poll(any(Duration.class))).thenReturn(mockScanRecords);

        List<InternalRow> result = PgTableScanner.scanTable(mockConnection, mockTable, null, 2);

        assertThat(result).hasSize(2);
        assertThat(result).containsExactly(mockRow1, mockRow2);
        verify(mockScanner).close();
    }

    @Test
    void testScanTableWithProjection() throws Exception {
        List<String> projectedColumns = Arrays.asList("id", "name");
        
        InternalRow mockRow = mock(InternalRow.class);
        ScanRecord mockRecord = mock(ScanRecord.class);
        when(mockRecord.getRow()).thenReturn(mockRow);

        TableBucket bucket = new TableBucket(0L, 0);

        when(mockScanRecords.count()).thenReturn(1).thenReturn(0);
        when(mockScanRecords.buckets()).thenReturn(Collections.singleton(bucket));
        when(mockScanRecords.records(bucket)).thenReturn(Collections.singletonList(mockRecord));
        when(mockScanner.poll(any(Duration.class))).thenReturn(mockScanRecords);

        List<InternalRow> result = PgTableScanner.scanTable(mockConnection, mockTable, projectedColumns, -1);

        assertThat(result).hasSize(1);
        verify(mockScan).project(projectedColumns);
        verify(mockScanner).close();
    }

    @Test
    void testScanTableWithEmptyProjectionList() throws Exception {
        List<String> projectedColumns = new ArrayList<>();
        
        InternalRow mockRow = mock(InternalRow.class);
        ScanRecord mockRecord = mock(ScanRecord.class);
        when(mockRecord.getRow()).thenReturn(mockRow);

        TableBucket bucket = new TableBucket(0L, 0);

        when(mockScanRecords.count()).thenReturn(1).thenReturn(0);
        when(mockScanRecords.buckets()).thenReturn(Collections.singleton(bucket));
        when(mockScanRecords.records(bucket)).thenReturn(Collections.singletonList(mockRecord));
        when(mockScanner.poll(any(Duration.class))).thenReturn(mockScanRecords);

        List<InternalRow> result = PgTableScanner.scanTable(mockConnection, mockTable, projectedColumns, -1);

        assertThat(result).hasSize(1);
        verify(mockScanner).close();
    }

    @Test
    void testIsScanFeasible() {
        TableInfo tableInfo = mock(TableInfo.class);
        boolean result = PgTableScanner.isScanFeasible(tableInfo);
        
        assertThat(result).isTrue();
    }

    @Test
    void testScannerClosedOnException() throws Exception {
        when(mockScanner.poll(any(Duration.class))).thenThrow(new RuntimeException("Test exception"));

        try {
            PgTableScanner.scanTable(mockConnection, mockTable, null, -1);
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).isEqualTo("Test exception");
        }

        verify(mockScanner).close();
    }
}
