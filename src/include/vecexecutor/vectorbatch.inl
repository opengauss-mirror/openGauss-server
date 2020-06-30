/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 * 
 * vectorbatch.inl
 *    Core data structure template implementation for vEngine
 * 
 * IDENTIFICATION
 *        src/include/vecexecutor/vectorbatch.inl
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef VECTORBATCH_INL
#define VECTORBATCH_INL


/*
 * @Description: If we call original Pack func to pack data.
 * There are unnecessarily operations that all column data will be moved.
 * An optimization is using OptimizePack func to move specific column data that we want.
 * @in sel - flag which row we should move.
 * @in Copyvars - flag which column we should move.
 * @template copyMatch - copy match pattern which always true.
 * @template hasSysCol - whether has System column data need to move.
 */
template <bool copyMatch, bool hasSysCol>
void VectorBatch::OptimizePackT(_in_ const bool * sel, _in_ List * CopyVars)
{
	int     i, j, writeIdx = 0;
	ScalarVector *pColumns = m_arr;
	int     cRows = m_rows;
	int     cColumns = m_cols;
	ScalarValue 	*pValues = NULL;
	uint8*			 pFlag = NULL;
	errno_t			 rc = EOK;

	Assert (IsValid());

	// Copy all values what we need indeed instead of copy whole table.
	//
	for (i = 0; i < cRows; i++)
	{
		bool    curSel = *sel ++;

		curSel = copyMatch ?curSel:!curSel;
		if (curSel)
		{
			if (i != writeIdx)
			{
				Assert (i > writeIdx);

				/*
				 * If we call original Pack func to pack data.
				 * There are unnecessarily operations that all column data will be moved.
				 * An optimization is using OptimizePack func to move specific column data that we want.
				 */
				ListCell *var = NULL;
				foreach (var, CopyVars)
				{
					int k = lfirst_int(var) - 1;
					pValues = pColumns[k].m_vals;
					pFlag = pColumns[k].m_flag;

					pValues[writeIdx] = pValues[i];
					pFlag[writeIdx] = pFlag[i];
				}

				if(hasSysCol)
				{
					Assert(m_sysColumns != NULL);
					for(j = 0 ; j < m_sysColumns->sysColumns; j++)
					{
						//sys column do not need null flag
						pValues = m_sysColumns->m_ppColumns[j].m_vals;
						pValues[writeIdx] = pValues[i];
					}
				}
			}

			writeIdx ++;
		}
	}

	for (j = 0; j < cColumns; j++)
	{
		pColumns[j].m_rows = writeIdx;
	}

	m_rows = writeIdx;
	Assert(m_rows >= 0 && m_rows <= BatchMaxSize);
	rc = memset_s(m_sel, BatchMaxSize * sizeof(bool), true, m_rows * sizeof(bool));
	securec_check(rc,"\0","\0");
	Assert (IsValid());	
}


/*
 * @Description: If we call original Pack func to pack data.
 * There are unnecessarily operations that all column data will be moved.
 * An optimization is using OptimizePackTForLateRead func to move specific column data 
 * that we want in later read situation.
 * @in sel - flag which row we should move.
 * @in lateVars - flag which column we should move in late read.
 * @in ctidColIdx - flag which ctid column data.
 * @template copyMatch - copy match pattern which always true.
 * @template hasSysCol - whether has System column data need to move.
 */
template <bool copyMatch, bool hasSysCol>
void VectorBatch::OptimizePackTForLateRead(_in_ const bool * sel, _in_ List * lateVars, int ctidColIdx)
{
	int     i, j, k, writeIdx = 0;
	ScalarVector *pColumns = m_arr;
	int     cRows = m_rows;
	int     cColumns = m_cols;
	ScalarValue 	*pValues = NULL;
	uint8*			 pFlag = NULL;
	errno_t			 rc = EOK;

	Assert (IsValid());

	// Copy all values what we need indeed instead of copy whole table.
	//
	for (i = 0; i < cRows; i++)
	{
		bool    curSel = *sel ++;

		curSel = copyMatch ?curSel:!curSel;
		if (curSel)
		{
			if (i != writeIdx)
			{
				Assert (i > writeIdx);

				/*
				 * If we call original Pack func to pack data.
				 * There are unnecessarily operations that all column data will be moved.
				 * An optimization is using OptimizePack func to move specific column data that we want.
				 */
				
				ListCell *var = NULL;
				foreach (var, lateVars)
				{
					k = lfirst_int(var) - 1;
					pValues = pColumns[k].m_vals;
					pFlag = pColumns[k].m_flag;

					pValues[writeIdx] = pValues[i];
					pFlag[writeIdx] = pFlag[i];
				}
				
				k = ctidColIdx;
				pValues = pColumns[k].m_vals;
				pFlag = pColumns[k].m_flag;

				pValues[writeIdx] = pValues[i];
				pFlag[writeIdx] = pFlag[i];

				if(hasSysCol)
				{
					Assert(m_sysColumns != NULL);
					for(j = 0 ; j < m_sysColumns->sysColumns; j++)
					{
						//sys column do not need null flag
						pValues = m_sysColumns->m_ppColumns[j].m_vals;
						pValues[writeIdx] = pValues[i];
					}
				}
			}
			writeIdx ++;
		}	
	}

	for (j = 0; j < cColumns; j++)
	{
		pColumns[j].m_rows = writeIdx;
	}

	m_rows = writeIdx;
	Assert(m_rows >= 0 && m_rows <= BatchMaxSize);
	rc = memset_s(m_sel, BatchMaxSize * sizeof(bool), true, m_rows * sizeof(bool));
	securec_check(rc,"\0","\0");
	Assert (IsValid());	
}



template <bool copyMatch, bool hasSysCol>
void VectorBatch::PackT (_in_ const bool *sel)
{
	int     i, j, writeIdx = 0;
	ScalarVector *pColumns = m_arr;
	int     cRows = m_rows;
	int     cColumns = m_cols;
	ScalarValue 	*pValues = NULL;
	uint8*			 pFlag = NULL;
	errno_t			 rc = EOK;

	Assert (IsValid());


	// Copy all values
	//
	for (i = 0; i < cRows; i++)
	{
		bool    curSel = *sel ++;

		curSel = copyMatch ?curSel:!curSel;
		if (curSel)
		{
			if (i != writeIdx)
			{
				Assert (i > writeIdx);

				for (j = 0; j < cColumns; j++)
				{
					pValues = pColumns[j].m_vals;
					pFlag = pColumns[j].m_flag;
					
					pValues[writeIdx] = pValues[i];
					pFlag[writeIdx] = pFlag[i];
				}

				if(hasSysCol)
				{
					Assert(m_sysColumns != NULL);
					for(j = 0 ; j < m_sysColumns->sysColumns; j++)
					{
						//sys column do not need null flag
						pValues = m_sysColumns->m_ppColumns[j].m_vals;
						pValues[writeIdx] = pValues[i];
					}
				}
			}

			writeIdx ++;
		}
	}

	// Restore constant columns
	//
	for (j = 0; j < cColumns; j++)
	{
		pColumns[j].m_rows = writeIdx;
	}

	m_rows = writeIdx;
	Assert(m_rows >= 0 && m_rows <= BatchMaxSize);
	rc = memset_s(m_sel, BatchMaxSize * sizeof(bool), true, m_rows * sizeof(bool));
	securec_check(rc,"\0","\0");
	Assert (IsValid());
}

#endif
