===============================================================
 Publish Result Tracking - ``kombu.transport.publish_tracking``
===============================================================


.. currentmodule:: kombu.transport.publish_tracking

.. automodule:: kombu.transport.publish_tracking

    .. contents::
        :local:

    Publish State
    -------------

    .. autoclass:: PublishState

        .. autoattribute:: PENDING
        .. autoattribute:: DELIVERED
        .. autoattribute:: RETURNED
        .. autoattribute:: NACKED
        .. autoattribute:: UNKNOWN

        .. automethod:: terminal

    Publish Result
    --------------

    .. autoclass:: PublishResult

        .. autoattribute:: publish_id
        .. autoattribute:: state
        .. autoattribute:: delivery_tag
        .. autoattribute:: exchange
        .. autoattribute:: routing_key
        .. autoattribute:: reply_code
        .. autoattribute:: reply_text

    Publish Tracker
    ---------------

    .. autoclass:: PublishTracker

        .. autoattribute:: pending
        .. autoattribute:: results

        .. automethod:: track
        .. automethod:: result_for
        .. automethod:: handle_ack
        .. automethod:: handle_nack
        .. automethod:: handle_return
        .. automethod:: unknown
        .. automethod:: expire_all
        .. automethod:: close

    Exceptions
    ----------

    .. autoexception:: PublishTrackingError
