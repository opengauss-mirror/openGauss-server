subdir = src/gausskernel/storage/access/datavec
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

override LDFLAGS += -L$(TOKENIZERS_LIB_PATH) -L$(ONNXRUNTIME_LIB_PATH)
override CXXFLAGS := -I$(ONNXRUNTIME_INCLUDE_PATH) -I$(TOKENIZERS_INCLUDE_PATH) $(CXXFLAGS)

ifneq "$(MAKECMDGOALS)" "clean"
  ifneq "$(MAKECMDGOALS)" "distclean"
     ifneq "$(shell which g++ |grep hutaf_llt |wc -l)" "1"
        -include $(DEPEND)
     endif
  endif
endif

OBJS = bitutils.o hnsw.o hnswbuild.o hnswdelete.o hnswinsert.o hnswscan.o hnswutils.o hnswvacuum.o \
       ivfbuild.o ivfflat.o ivfinsert.o ivfkmeans.o ivfscan.o ivfutils.o ivfvacuum.o vecindex.o \
	   utils.o hnswadaptor.o ivfadaptor.o bm25.o bm25build.o bm25scan.o bm25utils.o bm25vacuum.o varblock.o ivfnpuadaptor.o \
	   pg_prng.o diskann.o diskannbuild.o diskannscan.o diskannutils.o diskanndelete.o common_parallel.o \
       vectortransformer.o rabitq.o scalarquantizer.o ogai_model_framework.o ogai_model_manager.o ogai_qwen.o \
       ogai_utils.o ogai.o ogai_ollama.o ogai_openai.o ogai_launcher.o ogai_text_splitter.o \
       ogai_vector_processor.o ogai_worker.o ogai_onnx_mgr.o ogai_onnx_embedding.o ogai_onnx_runtime.o hnswlsg.o

include $(top_srcdir)/src/gausskernel/common.mk
