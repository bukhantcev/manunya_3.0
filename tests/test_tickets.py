import asyncio
import io
import json
import os
import importlib.util
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from pathlib import Path

from PIL import Image, ImageDraw
import pypdfium2 as pdfium
import httpx2
from openai import AsyncOpenAI, BadRequestError

from tickets import (Config, Extraction, ReadField, Journey, Ticket, TicketService, Recognizer,
                     LimitedBuffer, MAX_BYTES, belongs, caption, checks, image_mime, build_router, references, pdf_pages, MAX_PDF_PAGES)
from tickets import TicketUpdateObserver, audit_error, selected_ticket


def f(value=None, status=None):
    return ReadField(value=value, status=status or ('read' if value else 'missing'))


def journey(**kw):
    fields = dict(origin=f('Москва'), destination=f('Казань'), departure_date=f('25.09.2026'),
                  departure_time=f('12:30'), booking_code=f(), ticket_number=f())
    fields.update(kw)
    return Journey(**fields)


def ticket(**kw):
    fields = dict(kind='rail', surname=f('Иванов'), given_name=f('Иван'),
                  patronymic=f('Иванович'), passport=f('1234 567890'),
                  booking_code=f(), ticket_number=f(), journeys=[journey()])
    fields.update(kw)
    return Ticket(**fields)


def config():
    return Config('fake-key', 123456, frozenset({-100123}), 'Иванов', 'Иван', 'Иванович',
                  ('1234567890',), 'IVANOV', 'IVAN')


def extraction(t=None, **kw):
    return Extraction(tickets=[t or ticket()], uncertain=False, contains_other_personal_data=False).model_copy(update=kw)


def png():
    buf = io.BytesIO()
    Image.new('RGB', (10, 10)).save(buf, format='PNG')
    return buf.getvalue()


def pdf_fixture(colors):
    pages = [Image.new('RGB', (300, 150), color) for color in colors]
    try:
        for index, page in enumerate(pages, start=1):
            ImageDraw.Draw(page).text((20, 30), f'SYNTHETIC TICKET PAGE {index}', fill='black')
        with io.BytesIO() as buffer:
            pages[0].save(buffer, format='PDF', save_all=True, append_images=pages[1:], resolution=150)
            return buffer.getvalue()
    finally:
        for page in pages:
            page.close()


def message(mid=1, group=-100123, document=False, album=None):
    media = SimpleNamespace(file_id='fake-file', file_size=100, mime_type='image/png')
    return SimpleNamespace(chat=SimpleNamespace(id=group, type='supergroup'),
                           message_id=mid, media_group_id=album,
                           photo=[] if document else [media], document=media if document else None)


class ComparisonTests(unittest.TestCase):
    def test_error_telemetry_never_contains_api_body(self):
        exc = BadRequestError('SECRET FULL PAYLOAD', response=httpx2.Response(400,
            request=httpx2.Request('POST', 'https://api.openai.com/v1/responses')),
            body={'code': 'invalid_json_schema', 'message': 'SECRET PASSPORT'})
        with self.assertLogs('tickets', level='INFO') as logs:
            audit_error(exc)
        text = '\n'.join(logs.output)
        self.assertIn('type=BadRequestError http=400 code=invalid_json_schema', text)
        self.assertNotIn('SECRET', text)

    def test_selection_reasons_have_no_personal_data(self):
        cases = [(extraction(uncertain=True), 'skip_uncertain'),
                 (extraction(contains_other_personal_data=True), 'skip_other_personal_data'),
                 (extraction(tickets=[]), 'skip_no_ticket'),
                 (extraction(tickets=[ticket(), ticket()]), 'skip_multiple_passengers'),
                 (extraction(ticket(surname=f('Петров'), passport=f('9999999999'))), 'skip_identity_not_confirmed'),
                 (extraction(), 'identity_match')]
        for result, stage in cases:
            with self.assertLogs('tickets', level='INFO') as logs:
                selected_ticket(result, config())
            text = '\n'.join(logs.output)
            self.assertIn('stage=' + stage, text)
            for secret in ('Иванов', 'Петров', '9999999999', '123456'):
                self.assertNotIn(secret, text)

    def test_compact_one_way_caption(self):
        self.assertEqual(caption(ticket(), config()),
                         '25.09.2026 · 12:30 · Москва → Казань\n\nДокумент совпадает')

    def test_return_trip_keeps_separate_dates_and_times(self):
        t = ticket()
        t.journeys.append(journey(origin=f('Казань'), destination=f('Москва'), departure_date=f('30.09.2026'),
                                  departure_time=f('18:45')))
        self.assertEqual(caption(t, config()),
                         '1. 25.09.2026 · 12:30 · Москва → Казань\n'
                         '2. 30.09.2026 · 18:45 · Казань → Москва\n\nДокумент совпадает')

    def test_compact_document_status_is_honest(self):
        for document, expected in [(f('1234***890'), 'Документ не удалось прочитать'),
                                   (f(None, 'unreadable'), 'Документ не удалось прочитать'),
                                   (f(), 'Документ не указан'),
                                   (f('9999999999'), 'Документ: расхождение')]:
            text = caption(ticket(passport=document), config())
            self.assertTrue(text.endswith(expected))
            self.assertNotIn('Документ совпадает', text)

    def test_missing_journey_fields_are_not_guessed(self):
        t = ticket(journeys=[journey(departure_date=f(),
                                    departure_time=f(None, 'unreadable'))])
        self.assertEqual(caption(t, config()),
                         'Дата не указана · Время не удалось прочитать · Москва → Казань\n\nДокумент совпадает')
        self.assertIn('Время не указано', caption(ticket(journeys=[journey(
            origin=f(), destination=f(), departure_date=f(), departure_time=f())]), config()))

    def test_actual_name_discrepancy_still_visible(self):
        text = caption(ticket(surname=f('Ивапов')), config())
        self.assertIn('ФИО: расхождение', text)
        self.assertTrue(text.endswith('Документ совпадает'))

    def test_airports_and_shared_booking_appear_once(self):
        t = ticket(kind='air', booking_code=f('TEST01'), ticket_number=f('000-1234567890'), journeys=[
            journey(origin=f('Москва, Шереметьево (SVO)'), destination=f('Казань (KZN)'),
                    booking_code=f('TEST01')),
            journey(origin=f('Казань (KZN)'), destination=f('Москва, Шереметьево (SVO)'),
                    departure_date=f('30.09.2026'), departure_time=f('18:45')),
        ])
        text = caption(t, config())
        self.assertIn('Москва, Шереметьево (SVO) → Казань (KZN)', text)
        self.assertIn('Казань (KZN) → Москва, Шереметьево (SVO)', text)
        codes = references(t)
        self.assertEqual(codes, ['Бронь: <code>TEST01</code>', 'Билет: <code>000-1234567890</code>'])
        self.assertNotIn('TEST01', text)
        self.assertNotIn('000-1234567890', text)
        self.assertTrue(text.endswith('Документ совпадает'))

    def test_separate_rail_tickets_stay_with_their_legs(self):
        t = ticket(journeys=[
            journey(origin=f('Москва, Казанский вокзал'), destination=f('Казань-Пассажирская'),
                    booking_code=f('OUT001'), ticket_number=f('000111')),
            journey(origin=f('Казань-Пассажирская'), destination=f('Москва, Казанский вокзал'),
                    departure_date=f('30.09.2026'), departure_time=f('18:45'),
                    booking_code=f('BACK02'), ticket_number=f('000222')),
        ])
        text = caption(t, config())
        self.assertIn('Москва, Казанский вокзал → Казань-Пассажирская', text)
        self.assertIn('Казань-Пассажирская → Москва, Казанский вокзал', text)
        self.assertEqual(references(t), [
            'Бронь (направление 1): <code>OUT001</code>',
            'Билет (направление 1): <code>000111</code>',
            'Бронь (направление 2): <code>BACK02</code>',
            'Билет (направление 2): <code>000222</code>',
        ])

    def test_unreadable_reference_not_printed_and_missing_omitted(self):
        t = ticket(booking_code=f('DO-NOT-USE', 'unreadable'), ticket_number=f())
        self.assertNotIn('DO-NOT-USE', caption(t, config()))
        self.assertEqual(references(t), [])


    def test_exact_match(self):
        self.assertTrue(belongs(ticket(), config()))
        self.assertEqual(set(checks(ticket(), config()).values()), {'совпадает'})

    def test_passport_mismatch_still_reports(self):
        t = ticket(passport=f('1234567891'))
        self.assertTrue(belongs(t, config()))
        self.assertEqual(checks(t, config())['Документ'], 'расхождение')
        self.assertNotIn('1234567891', caption(t, config()))

    def test_exact_document_name_typo(self):
        t = ticket(surname=f('Ивапов'))
        self.assertTrue(belongs(t, config()))
        self.assertEqual(checks(t, config())['Фамилия'], 'расхождение')

    def test_foreign_and_initials(self):
        for name in ['Петров', 'И', 'Ивано', 'Ivanoff']:
            self.assertFalse(belongs(ticket(surname=f(name), passport=f()), config()))
        self.assertFalse(belongs(ticket(given_name=f('И.'), passport=f()), config()))

    def test_explicit_latin_air(self):
        t = ticket(kind='air', surname=f('IVANOV'), given_name=f('IVAN'), patronymic=f(), passport=f())
        self.assertTrue(belongs(t, config()))
        self.assertEqual(checks(t, config())['Отчество'], 'поле отсутствует')
        self.assertFalse(belongs(t.model_copy(update={'kind': 'rail'}), config()))
        self.assertFalse(belongs(t.model_copy(update={'given_name': f('IWAN')}), config()))

    def test_unreadable_patronymic_not_absent(self):
        self.assertFalse(belongs(ticket(kind='air', passport=f(), patronymic=f(None, 'unreadable')), config()))

    def test_masked_and_letter_zero_not_guessed(self):
        self.assertEqual(checks(ticket(passport=f('1234***890')), config())['Документ'], 'не удалось прочитать')
        self.assertEqual(checks(ticket(passport=f('123456789O')), config())['Документ'], 'расхождение')
        self.assertEqual(checks(ticket(passport=f()), config())['Документ'], 'поле отсутствует')
        self.assertEqual(checks(ticket(passport=f('1234567890', 'unreadable')), config())['Документ'], 'не удалось прочитать')

    def test_config_disabled_and_validation(self):
        self.assertIsNone(Config.from_env({}, []))
        with self.assertRaises(ValueError):
            Config.from_env({'TICKETS_ENABLED': 'true'}, [])
        env = dict(TICKETS_ENABLED='true', OPENAI_API_KEY='fake', TICKET_RECIPIENT_ID='123456',
                   TICKET_GROUP_IDS='-100123', TICKET_SURNAME='Иванов', TICKET_GIVEN_NAME='Иван',
                   TICKET_PATRONYMIC='Иванович', TICKET_PASSPORTS='1234 567890')
        self.assertEqual(Config.from_env(env, [-100123]).recipient, 123456)
        for change in [{'TICKET_RECIPIENT_ID': '-123'}, {'TICKET_GROUP_IDS': '-999'},
                       {'TICKET_PASSPORTS': '12**'}, {'TICKET_LATIN_SURNAME': 'IVANOV'}]:
            with self.assertRaises(ValueError):
                Config.from_env(env | change, [-100123])

    def test_image_validation_and_limited_buffer(self):
        self.assertEqual(image_mime(png()), 'image/png')
        with self.assertRaises(Exception):
            image_mime(b'not image')
        with LimitedBuffer() as b:
            b.seek(MAX_BYTES)
            with self.assertRaises(ValueError):
                b.write(b'x')


class HandlingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.bot = SimpleNamespace(get_chat_member=AsyncMock(return_value=SimpleNamespace(status='member')),
                                   send_message=AsyncMock(), send_photo=AsyncMock(), send_document=AsyncMock())
        self.buffers = []
        async def download(file_id, destination):
            self.buffers.append(destination)
            destination.write(png())
        self.bot.download = AsyncMock(side_effect=download)
        self.recognize = AsyncMock(return_value=extraction())
        self.service = TicketService(config(), self.recognize)

    async def test_update_observer_logs_only_media_kind(self):
        observer = TicketUpdateObserver()
        handler = AsyncMock(return_value='ok')
        update = SimpleNamespace(message=message(), edited_message=None)
        with self.assertLogs('tickets', level='INFO') as logs:
            self.assertEqual(await observer(handler, update, {}), 'ok')
        text = '\n'.join(logs.output)
        self.assertIn('job=1 stage=update_photo', text)
        self.assertNotIn('-100123', text)
        self.assertNotIn('fake-file', text)
        handler.assert_awaited_once_with(update, {})

    async def test_delivery_stages_are_observable_without_payload(self):
        with self.assertLogs('tickets', level='INFO') as logs:
            await self.service.handle(message(), self.bot)
        text = '\n'.join(logs.output)
        for stage in ('handler_received', 'download_started', 'download_completed',
                      'identity_match', 'ticket_delivered', 'delivery_completed', 'processing_finished'):
            self.assertIn('stage=' + stage, text)
        for secret in ('Иванов', 'Москва', '123456', 'fake-file', '-100123'):
            self.assertNotIn(secret, text)

    async def test_send_and_repeat_message(self):
        await asyncio.gather(self.service.handle(message(), self.bot), self.service.handle(message(), self.bot))
        self.recognize.assert_awaited_once()
        self.bot.send_photo.assert_awaited_once()
        self.assertEqual(self.bot.send_photo.call_args.args[0], 123456)
        self.assertTrue(self.buffers[0].closed)
        self.assertEqual(list(self.service.seen), [(-100123, 1)])
        self.assertNotIn('passport', vars(self.service))

    async def test_document(self):
        await self.service.handle(message(document=True), self.bot)
        self.bot.send_document.assert_awaited_once()
        self.bot.send_photo.assert_not_awaited()

    async def test_group_and_recipient_membership(self):
        await self.service.handle(message(group=-999), self.bot)
        self.bot.download.assert_not_awaited()
        self.bot.get_chat_member.return_value = SimpleNamespace(status='left')
        await self.service.handle(message(), self.bot)
        self.recognize.assert_not_awaited()

    async def test_membership_rechecked_before_send(self):
        self.bot.get_chat_member.side_effect = [SimpleNamespace(status='member'), SimpleNamespace(status='left')]
        await self.service.handle(message(), self.bot)
        self.bot.send_photo.assert_not_awaited()

    async def test_album_does_not_borrow_identity(self):
        self.recognize.side_effect = [extraction(), extraction(ticket(surname=f('Петров'), passport=f('9999999999')))]
        await self.service.handle(message(1, album='album'), self.bot)
        await self.service.handle(message(2, album='album'), self.bot)
        self.assertEqual(self.recognize.await_count, 2)
        self.bot.send_photo.assert_awaited_once()
        self.bot.send_message.assert_not_awaited()

    async def test_same_passenger_return_trip_is_sent_once(self):
        t = ticket()
        t.journeys.append(journey(origin=f('Казань'), destination=f('Москва'), departure_date=f('30.09.2026'),
                                  departure_time=f('18:45')))
        self.recognize.return_value = extraction(t)
        await self.service.handle(message(), self.bot)
        self.bot.send_photo.assert_awaited_once()
        text = self.bot.send_photo.call_args.kwargs['caption']
        self.assertIn('25.09.2026 · 12:30', text)
        self.assertIn('30.09.2026 · 18:45', text)
        self.bot.send_message.assert_not_awaited()

    async def test_codes_are_separate_copyable_messages(self):
        self.recognize.return_value = extraction(ticket(booking_code=f('TEST01'), ticket_number=f('001234')))
        await self.service.handle(message(), self.bot)
        self.assertNotIn('TEST01', self.bot.send_photo.call_args.kwargs['caption'])
        self.assertEqual([c.args[1] for c in self.bot.send_message.call_args_list],
                         ['Бронь: <code>TEST01</code>', 'Билет: <code>001234</code>'])
        self.assertTrue(all(c.kwargs['parse_mode'] == 'HTML' for c in self.bot.send_message.call_args_list))

    async def test_ten_photo_batch_sends_only_owned_ticket(self):
        foreign = extraction(ticket(surname=f('Петров'), passport=f('9999999999')))
        self.recognize.side_effect = [foreign] * 4 + [extraction()] + [foreign] * 5
        for i in range(10):
            await self.service.handle(message(i + 1, album='batch'), self.bot)
        self.bot.send_photo.assert_awaited_once()
        self.bot.send_message.assert_not_awaited()

    async def test_mixed_pdf_sends_only_owned_page_as_pdf(self):
        payload = pdf_fixture(['red', 'white', 'blue'])
        async def download(file_id, destination):
            self.buffers.append(destination)
            destination.write(payload)
        self.bot.download.side_effect = download
        self.recognize.side_effect = [
            extraction(ticket(surname=f('Петров'), passport=f('9999999999'))),
            extraction(), extraction(uncertain=True),
        ]
        msg = message(document=True)
        msg.document.mime_type = 'application/pdf'
        await self.service.handle(msg, self.bot)
        self.bot.send_document.assert_awaited_once()
        sent = self.bot.send_document.call_args.args[1]
        self.assertEqual(sent.filename, 'ticket-page-2.pdf')
        with pdfium.PdfDocument(sent.data) as doc:
            self.assertEqual(len(doc), 1)
            page = doc[0]
            bitmap = page.render()
            try:
                with bitmap.to_pil() as image:
                    self.assertTrue(all(v > 240 for v in image.convert('RGB').getpixel((2, 2))))
            finally:
                bitmap.close()
                page.close()
        self.assertTrue(self.buffers[0].closed)
        self.bot.send_message.assert_not_awaited()
        self.assertTrue(all(c.args[1] == 'image/jpeg' for c in self.recognize.call_args_list))

    async def test_own_pdf_pages_kept_and_common_codes_not_repeated(self):
        payload = pdf_fixture(['white', 'white'])
        async def download(file_id, destination):
            destination.write(payload)
        self.bot.download.side_effect = download
        self.recognize.return_value = extraction(ticket(booking_code=f('TEST01')))
        msg = message(document=True)
        msg.document.mime_type = 'application/pdf'
        await self.service.handle(msg, self.bot)
        self.assertEqual(self.bot.send_document.await_count, 2)
        self.assertEqual(self.bot.send_message.await_count, 1)

    async def test_pdf_foreign_mixed_people_and_uncertain_are_silent(self):
        payload = pdf_fixture(['white'] * 3)
        async def download(file_id, destination):
            destination.write(payload)
        self.bot.download.side_effect = download
        self.recognize.side_effect = [extraction(tickets=[ticket(), ticket()]),
                                     extraction(uncertain=True), extraction(tickets=[])]
        msg = message(document=True)
        msg.document.mime_type = 'application/pdf'
        await self.service.handle(msg, self.bot)
        self.bot.send_document.assert_not_awaited()
        self.bot.send_message.assert_not_awaited()

    async def test_pdf_limit_and_broken_pdf_are_silent(self):
        msg = message(document=True)
        msg.document.mime_type = 'application/pdf'
        for index, payload in enumerate([b'%PDF-invalid', pdf_fixture(['white'] * (MAX_PDF_PAGES + 1))]):
            async def download(file_id, destination):
                destination.write(payload)
            self.bot.download.side_effect = download
            msg.message_id = index + 1
            with self.assertLogs('tickets', level='WARNING'):
                await self.service.handle(msg, self.bot)
        self.recognize.assert_not_awaited()
        self.bot.send_document.assert_not_awaited()
        self.bot.send_message.assert_not_awaited()

    async def test_other_passenger_on_return_coupon_is_not_sent(self):
        self.recognize.return_value = extraction(tickets=[ticket(), ticket(
            surname=f('Петров'), passport=f('9999999999'))])
        await self.service.handle(message(), self.bot)
        self.bot.send_photo.assert_not_awaited()
        self.bot.send_message.assert_not_awaited()

    async def test_long_itinerary_keeps_every_leg_within_telegram_limits(self):
        t = ticket(journeys=[journey(origin=f('🚄' * 150), departure_date=f(str(i)),
                                    departure_time=f('12:30')) for i in range(20)])
        self.recognize.return_value = extraction(t)
        await self.service.handle(message(), self.bot)
        self.bot.send_photo.assert_awaited_once()
        self.assertIsNone(self.bot.send_photo.call_args.kwargs['caption'])
        parts = [call.args[1] for call in self.bot.send_message.call_args_list]
        self.assertEqual(''.join(parts), caption(t, config()))
        self.assertTrue(all(len(part.encode('utf-16-le')) // 2 <= 4000 for part in parts))

    async def test_uncertain_mixed_and_other_data_not_sent(self):
        for i, update in enumerate([{'uncertain': True}, {'tickets': [ticket(), ticket()]},
                                    {'contains_other_personal_data': True}], start=1):
            self.recognize.return_value = extraction(**update)
            await self.service.handle(message(i), self.bot)
        self.bot.send_photo.assert_not_awaited()
        self.bot.send_message.assert_not_awaited()

    async def test_non_ticket(self):
        self.recognize.return_value = extraction(tickets=[])
        await self.service.handle(message(), self.bot)
        self.bot.send_message.assert_not_awaited()
        self.bot.send_photo.assert_not_awaited()

    async def test_failure_does_not_log_content_and_closes_buffer(self):
        self.recognize.side_effect = RuntimeError('SECRET PASSPORT OCR')
        with self.assertLogs('tickets', level='WARNING') as logs:
            await self.service.handle(message(), self.bot)
        self.assertNotIn('SECRET', ''.join(logs.output))
        self.assertTrue(self.buffers[0].closed)
        self.bot.send_photo.assert_not_awaited()
        self.bot.send_message.assert_not_awaited()

    async def test_delivery_failure_no_automatic_repeat(self):
        self.bot.send_photo.side_effect = TimeoutError('private content')
        with self.assertLogs('tickets', level='WARNING'):
            await self.service.handle(message(), self.bot)
        await self.service.handle(message(), self.bot)
        self.bot.send_photo.assert_awaited_once()

    async def test_new_message_can_retry(self):
        self.recognize.side_effect = [RuntimeError(), extraction()]
        with self.assertLogs('tickets', level='WARNING'):
            await self.service.handle(message(), self.bot)
        await self.service.handle(message(2), self.bot)
        self.bot.send_photo.assert_awaited_once()

    async def test_router_and_api_privacy(self):
        self.assertEqual(build_router(self.service).name, 'tickets')
        recognizer = Recognizer(config())
        await recognizer.close()
        parse = AsyncMock(return_value=SimpleNamespace(status='completed', output_parsed=extraction()))
        recognizer.client = SimpleNamespace(responses=SimpleNamespace(parse=parse))
        await recognizer(png(), 'image/png')
        args = parse.call_args.kwargs
        self.assertIs(args['store'], False)
        self.assertEqual(args['model'], 'gpt-5.6-sol')
        self.assertEqual(args['reasoning'], {'effort': 'medium'})
        self.assertEqual(args['max_output_tokens'], 25000)
        self.assertNotIn('1234567890', str(args['input']))
        self.assertNotIn('Иванов', str(args['input']))
        self.assertTrue(args['input'][1]['content'][0]['image_url'].startswith('data:image/png;base64,'))
        parse.return_value = SimpleNamespace(status='incomplete', output_parsed=None)
        with self.assertRaises(ValueError):
            await recognizer(png(), 'image/png')

    async def test_id_expiry_does_not_delete_new_entry(self):
        key = (-100123, 1)
        self.service.seen[key] = 200
        self.service.expire(key, 100)
        self.assertIn(key, self.service.seen)
        self.service.expire(key, 200)
        self.assertNotIn(key, self.service.seen)

    async def test_sdk_serializes_and_parses_without_network(self):
        def respond(request):
            body = json.loads(request.content)
            self.assertFalse(body['store'])
            self.assertEqual(body['model'], 'gpt-5.6-sol')
            self.assertEqual(body['reasoning'], {'effort': 'medium'})
            self.assertTrue(body['text']['format']['strict'])
            self.assertEqual(body['text']['format']['schema']['additionalProperties'], False)
            return httpx2.Response(200, json={
                'id': 'resp_fake', 'object': 'response', 'created_at': 1,
                'model': 'gpt-5.6-sol', 'status': 'completed',
                'output': [{'id': 'msg_fake', 'type': 'message', 'role': 'assistant',
                            'status': 'completed', 'content': [{'type': 'output_text',
                            'text': extraction().model_dump_json(), 'annotations': []}]}],
            })
        r = Recognizer(config())
        await r.close()
        r.client = AsyncOpenAI(api_key='fake', http_client=httpx2.AsyncClient(
            transport=httpx2.MockTransport(respond)), max_retries=0)
        try:
            result = await r(png(), 'image/png')
            self.assertEqual(result, extraction())
        finally:
            await r.close()


class ExistingBotTests(unittest.TestCase):
    def test_import_and_existing_handlers_without_env_or_network(self):
        # Do not read the user's .env or inherit their credentials.
        with patch.dict(os.environ, {}, clear=True), patch('dotenv.load_dotenv') as dotenv:
            spec = importlib.util.spec_from_file_location('test_main', Path(__file__).parents[1] / 'main.py')
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            self.assertIsNone(module.BOT_TOKEN)
            self.assertEqual(len(module.router.message.handlers), 3)
            self.assertEqual(len(module.router.callback_query.handlers), 1)
            self.assertTrue(callable(module.cmd_go))
            self.assertTrue(callable(module.on_cc))
            dotenv.assert_called_once()


if __name__ == '__main__':
    unittest.main()
