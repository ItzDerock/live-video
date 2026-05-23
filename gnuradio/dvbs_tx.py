#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#
# SPDX-License-Identifier: GPL-3.0
#
# GNU Radio Python Flow Graph
# Title: Dvbs Tx
# GNU Radio version: 3.10.12.0

from gnuradio import blocks
from gnuradio import dtv
from gnuradio import filter
from gnuradio.filter import firdes
from gnuradio import gr
from gnuradio.fft import window
import sys
import signal
from argparse import ArgumentParser
from gnuradio.eng_arg import eng_float, intx
from gnuradio import eng_notation
from gnuradio import iio
from gnuradio import zeromq
import threading




class dvbs_tx(gr.top_block):

    def __init__(self):
        gr.top_block.__init__(self, "Dvbs Tx", catch_exceptions=True)
        self.flowgraph_started = threading.Event()

        ##################################################
        # Variables
        ##################################################
        self.symbol_rate = symbol_rate = 3000000
        self.samp_rate = samp_rate = symbol_rate * 2
        self.rrc_taps = rrc_taps = 100
        self.center_freq = center_freq = int(3400e6)

        ##################################################
        # Blocks
        ##################################################

        self.zeromq_pull_source_0 = zeromq.pull_source(gr.sizeof_char, 1, 'ipc:///run/user/1000/raptorq.sock', 100, False, (-1), False)
        self.iio_pluto_sink_0_0 = iio.fmcomms2_sink_fc32('ip:pluto.local' if 'ip:pluto.local' else iio.get_pluto_uri(), [True, True], 1048576, False)
        self.iio_pluto_sink_0_0.set_len_tag_key('')
        self.iio_pluto_sink_0_0.set_bandwidth(20000000)
        self.iio_pluto_sink_0_0.set_frequency(center_freq)
        self.iio_pluto_sink_0_0.set_samplerate(samp_rate)
        self.iio_pluto_sink_0_0.set_attenuation(0, 0)
        self.iio_pluto_sink_0_0.set_filter_params('Auto', '', 0, 0)
        self.fft_filter_xxx_0 = filter.fft_filter_ccc(1, firdes.root_raised_cosine(1.0, samp_rate, samp_rate/2, 0.35, rrc_taps), 1)
        self.fft_filter_xxx_0.declare_sample_delay(0)
        self.dtv_dvbt_reed_solomon_enc_0 = dtv.dvbt_reed_solomon_enc(2, 8, 0x11d, 255, 239, 8, 51, 8)
        self.dtv_dvbt_inner_coder_0 = dtv.dvbt_inner_coder(1, 1512, dtv.MOD_QPSK, dtv.NH, dtv.C3_4)
        self.dtv_dvbt_energy_dispersal_0 = dtv.dvbt_energy_dispersal(1)
        self.dtv_dvbt_convolutional_interleaver_0 = dtv.dvbt_convolutional_interleaver(136, 12, 17)
        self.dtv_dvbs2_modulator_bc_0 = dtv.dvbs2_modulator_bc(
            dtv.FECFRAME_NORMAL,
            dtv.C1_4,
            dtv.MOD_QPSK,
            dtv.INTERPOLATION_ON)
        self.blocks_vector_to_stream_0 = blocks.vector_to_stream(gr.sizeof_char*1, 1512)


        ##################################################
        # Connections
        ##################################################
        self.connect((self.blocks_vector_to_stream_0, 0), (self.dtv_dvbs2_modulator_bc_0, 0))
        self.connect((self.dtv_dvbs2_modulator_bc_0, 0), (self.fft_filter_xxx_0, 0))
        self.connect((self.dtv_dvbt_convolutional_interleaver_0, 0), (self.dtv_dvbt_inner_coder_0, 0))
        self.connect((self.dtv_dvbt_energy_dispersal_0, 0), (self.dtv_dvbt_reed_solomon_enc_0, 0))
        self.connect((self.dtv_dvbt_inner_coder_0, 0), (self.blocks_vector_to_stream_0, 0))
        self.connect((self.dtv_dvbt_reed_solomon_enc_0, 0), (self.dtv_dvbt_convolutional_interleaver_0, 0))
        self.connect((self.fft_filter_xxx_0, 0), (self.iio_pluto_sink_0_0, 0))
        self.connect((self.zeromq_pull_source_0, 0), (self.dtv_dvbt_energy_dispersal_0, 0))


    def get_symbol_rate(self):
        return self.symbol_rate

    def set_symbol_rate(self, symbol_rate):
        self.symbol_rate = symbol_rate
        self.set_samp_rate(self.symbol_rate * 2)

    def get_samp_rate(self):
        return self.samp_rate

    def set_samp_rate(self, samp_rate):
        self.samp_rate = samp_rate
        self.fft_filter_xxx_0.set_taps(firdes.root_raised_cosine(1.0, self.samp_rate, self.samp_rate/2, 0.35, self.rrc_taps))
        self.iio_pluto_sink_0_0.set_samplerate(self.samp_rate)

    def get_rrc_taps(self):
        return self.rrc_taps

    def set_rrc_taps(self, rrc_taps):
        self.rrc_taps = rrc_taps
        self.fft_filter_xxx_0.set_taps(firdes.root_raised_cosine(1.0, self.samp_rate, self.samp_rate/2, 0.35, self.rrc_taps))

    def get_center_freq(self):
        return self.center_freq

    def set_center_freq(self, center_freq):
        self.center_freq = center_freq
        self.iio_pluto_sink_0_0.set_frequency(self.center_freq)




def main(top_block_cls=dvbs_tx, options=None):
    tb = top_block_cls()

    def sig_handler(sig=None, frame=None):
        tb.stop()
        tb.wait()

        sys.exit(0)

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    tb.start()
    tb.flowgraph_started.set()

    try:
        input('Press Enter to quit: ')
    except EOFError:
        pass
    tb.stop()
    tb.wait()


if __name__ == '__main__':
    main()
